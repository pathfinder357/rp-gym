package com.workoutdone.rpgym.game.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.workoutdone.rpgym.game.quest.adapter.in.kafka.ContractViolationException;
import com.workoutdone.rpgym.game.quest.adapter.in.kafka.HealthEventConsumer;
import com.workoutdone.rpgym.game.quest.application.PartyQuestProgressService;
import com.workoutdone.rpgym.game.quest.application.QuestProgressService;
import com.workoutdone.rpgym.game.quest.application.QuestSuggestionService;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.FixedBackOff;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLTransientConnectionException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// R-1 검증: 실제 브로커(내장 Kafka) 위에서 에러 핸들러가 레코드를 어디로 보내는지 본다.
// 파티션을 하나로 두어 모든 레코드가 한 줄로 선다. 예전 설정이라면 첫 실패에서 뒤가 전부 막힌다.
// 서비스는 목으로 대체하고 컨슈머 · 에러 핸들러 · DLT 발행만 진짜로 돈다.
@SpringJUnitConfig
@EmbeddedKafka(partitions = 1, topics = {HealthEventDeadLetterTest.TOPIC, HealthEventDeadLetterTest.DLT,
        HealthEventDeadLetterTest.BROKEN_DLT_TOPIC})
class HealthEventDeadLetterTest {

    static final String TOPIC = "health.events";
    static final String DLT = "health.events.DLT";
    static final String BROKEN_DLT_TOPIC = "health.events.broken-dlt-test";
    private static final String GROUP = "game-service-dlt-test";

    private static final UUID NORMAL_USER = UUID.randomUUID();
    private static final UUID BUG_USER = UUID.randomUUID();
    private static final UUID TRANSIENT_USER = UUID.randomUUID();

    @Autowired
    private EmbeddedKafkaBroker broker;

    private KafkaMessageListenerContainer<String, String> container;

    @AfterEach
    void tearDown() {
        if (container != null) {
            container.stop();
        }
    }

    private static String synced(UUID userId) {
        return """
                {"eventId":"%s","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"%s",
                 "data":{"activityDate":"2026-08-28","measuredAt":"2026-08-28T10:30:00+09:00",
                         "cumulative":{"steps":3100,"activeMinutes":31,"activeCalories":155}}}
                """.formatted(UUID.randomUUID(), userId);
    }

    @Test
    @DisplayName("계약 위반은 곧바로, 버그는 몇 번 뒤 DLT로 가고, 일시 장애는 DLT로 가지 않으며, 뒤의 정상 레코드는 처리된다")
    void 실패_종류별로_DLT_경로가_다르다() {
        QuestProgressService questProgressService = mock(QuestProgressService.class);
        when(questProgressService.apply(eq(BUG_USER), any()))
                .thenThrow(new IllegalStateException("버그 흉내: 항상 같은 자리에서 터진다"));
        CannotCreateTransactionException dbDown = new CannotCreateTransactionException(
                "Could not open JPA EntityManager",
                new SQLTransientConnectionException("HikariPool-1 - Connection is not available"));
        // 재시도 한도(2회)보다 오래 실패해도 일시 장애라 DLT로 가지 않고 결국 처리된다.
        when(questProgressService.apply(eq(TRANSIENT_USER), any()))
                .thenThrow(dbDown, dbDown, dbDown, dbDown, dbDown)
                .thenReturn(Optional.empty());

        HealthEventConsumer consumer = new HealthEventConsumer(
                new ObjectMapper().registerModule(new JavaTimeModule()),
                questProgressService, mock(PartyQuestProgressService.class), mock(QuestSuggestionService.class));

        Map<Long, AtomicInteger> attempts = new ConcurrentHashMap<>();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        KafkaTemplate<String, String> template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));

        ContainerProperties props = new ContainerProperties(TOPIC);
        props.setAckMode(ContainerProperties.AckMode.RECORD);
        props.setMessageListener((MessageListener<String, String>) record -> {
            attempts.computeIfAbsent(record.offset(), k -> new AtomicInteger()).incrementAndGet();
            consumer.consume(record.value());
        });
        container = new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(
                KafkaTestUtils.consumerProps(GROUP, "false", broker),
                new StringDeserializer(), new StringDeserializer()), props);
        container.setCommonErrorHandler(KafkaConsumerConfig.errorHandler(template, registry,
                new FixedBackOff(10L, FixedBackOff.UNLIMITED_ATTEMPTS), new FixedBackOff(10L, 2L)));
        container.start();

        String key = "same-partition";
        template.send(TOPIC, key, "{ this is not json");                              // 0 계약 위반
        template.send(TOPIC, key, """
                {"eventId":"%s","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"%s",
                 "data":{"activityDate":"2026-08-28","measuredAt":"2026-08-28T10:30:00+09:00"}}
                """.formatted(UUID.randomUUID(), NORMAL_USER));                         // 1 계약 위반(cumulative 없음)
        template.send(TOPIC, key, """
                {"eventId":"%s","eventType":"SOMETHING_NEW","userId":"%s","data":{}}
                """.formatted(UUID.randomUUID(), NORMAL_USER));                         // 2 모르는 타입: 넘긴다
        template.send(TOPIC, key, synced(BUG_USER));                                   // 3 버그
        template.send(TOPIC, key, synced(TRANSIENT_USER));                             // 4 일시 장애
        template.send(TOPIC, key, synced(NORMAL_USER));                                // 5 정상
        template.flush();

        // 원본 오프셋이 끝까지 커밋되면 lag 0
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(KafkaTestUtils.getCurrentOffset(broker.getBrokersAsString(), GROUP, TOPIC, 0))
                        .isNotNull()
                        .extracting(o -> o.offset()).isEqualTo(6L));

        verify(questProgressService).apply(eq(NORMAL_USER), any());

        List<ConsumerRecord<String, String>> dead = readDlt();
        assertThat(dead).extracting(HealthEventDeadLetterTest::originalOffset).containsExactly(0L, 1L, 3L);
        assertThat(header(dead.get(0), KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo(ContractViolationException.class.getName());
        assertThat(header(dead.get(2), KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .isEqualTo(IllegalStateException.class.getName());
        assertThat(dead.get(0).value()).isEqualTo("{ this is not json");

        // 계약 위반은 재시도 없이 한 번, 버그는 처음 + 재시도 2번, 일시 장애는 실패 5번 + 성공 1번
        assertThat(attempts.get(0L).get()).isEqualTo(1);
        assertThat(attempts.get(1L).get()).isEqualTo(1);
        assertThat(attempts.get(3L).get()).isEqualTo(3);
        assertThat(attempts.get(4L).get()).isEqualTo(6);

        assertThat(registry.get(KafkaConsumerConfig.DLT_COUNTER).counter().count()).isEqualTo(3.0);
    }

    @Test
    @DisplayName("DLT 발행이 실패하면 원본 오프셋을 커밋하지 않고 같은 레코드를 다시 처리한다 — 조용한 유실 0")
    void DLT_발행이_실패하면_오프셋을_넘기지_않는다() throws Exception {
        HealthEventConsumer consumer = new HealthEventConsumer(new ObjectMapper(),
                mock(QuestProgressService.class), mock(PartyQuestProgressService.class),
                mock(QuestSuggestionService.class));

        // DLT 쪽 프로듀서만 없는 브로커를 바라본다. 원본 토픽 소비는 정상이다.
        Map<String, Object> deadProducer = KafkaTestUtils.producerProps("localhost:1");
        deadProducer.put("max.block.ms", 200);
        KafkaTemplate<String, String> brokenDlt = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                deadProducer, new StringSerializer(), new StringSerializer()));
        KafkaTemplate<String, String> source = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                KafkaTestUtils.producerProps(broker), new StringSerializer(), new StringSerializer()));

        String topic = BROKEN_DLT_TOPIC;
        String group = GROUP + "-broken-dlt";
        AtomicInteger attempts = new AtomicInteger();
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        ContainerProperties props = new ContainerProperties(topic);
        props.setAckMode(ContainerProperties.AckMode.RECORD);
        props.setMessageListener((MessageListener<String, String>) record -> {
            attempts.incrementAndGet();
            consumer.consume(record.value());
        });
        container = new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(
                KafkaTestUtils.consumerProps(group, "false", broker),
                new StringDeserializer(), new StringDeserializer()), props);
        container.setCommonErrorHandler(KafkaConsumerConfig.errorHandler(brokenDlt, registry,
                new FixedBackOff(10L, FixedBackOff.UNLIMITED_ATTEMPTS), new FixedBackOff(10L, 2L)));
        container.start();

        source.send(topic, "k", "{ broken").join();

        // 계약 위반이라 매번 곧바로 DLT 발행을 시도하고, 실패할 때마다 같은 레코드를 다시 집는다.
        await().atMost(Duration.ofSeconds(20)).until(() -> attempts.get() >= 3);
        // 컨테이너가 할당 시점의 시작 위치(0)를 커밋해 둘 수는 있다. 깨진 레코드(오프셋 0)를 넘어가지만 않으면 된다.
        var committed = KafkaTestUtils.getCurrentOffset(broker.getBrokersAsString(), group, topic, 0);
        assertThat(committed == null ? 0L : committed.offset()).isZero();
        assertThat(registry.get(KafkaConsumerConfig.DLT_COUNTER).counter().count()).isZero();
    }

    private List<ConsumerRecord<String, String>> readDlt() {
        Map<String, Object> props = KafkaTestUtils.consumerProps(GROUP + "-dlt-reader", "false", broker);
        try (Consumer<String, String> reader = new DefaultKafkaConsumerFactory<>(
                props, new StringDeserializer(), new StringDeserializer()).createConsumer()) {
            broker.consumeFromAnEmbeddedTopic(reader, DLT);
            List<ConsumerRecord<String, String>> all = new ArrayList<>();
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                reader.poll(Duration.ofMillis(200)).forEach(all::add);
                return all.size() >= 3;
            });
            // 더 들어오는 것이 없는지 한 번 더 본다
            reader.poll(Duration.ofMillis(500)).forEach(all::add);
            return all;
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    private static long originalOffset(ConsumerRecord<?, ?> record) {
        return ByteBuffer.wrap(record.headers().lastHeader(KafkaHeaders.DLT_ORIGINAL_OFFSET).value()).getLong();
    }
}
