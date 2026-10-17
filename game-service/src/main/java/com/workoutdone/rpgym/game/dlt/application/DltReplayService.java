package com.workoutdone.rpgym.game.dlt.application;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * 흐름: 운영자가 원인을 고쳐 배포한다 -> dryRun 으로 무엇이 있는지 본다 -> 재처리를 부른다.
 *   - DLT 를 전용 컨슈머 그룹(game-service-dlt-replay)으로 처음부터 차례대로 읽는다. 어디까지 되돌렸는지는
 *     그 그룹의 커밋 오프셋이 기억한다. 같은 레코드를 두 번 되돌리지 않는다.
 *   - 원래 토픽에 **같은 key(userId) · 같은 value(eventId 포함 원문)** 로 다시 발행한다.
 *     파티션이 같아져 사용자별 순서가 유지되고, eventId 가 같아 소비 측 멱등 처리가 그대로 걸린다.
 *   - 발행이 브로커 응답까지 성공한 레코드까지만 오프셋을 커밋한다. 중간에 실패하면 거기서 멈춘다(유실 0).
 *
 * 무한 순환 방지: 고쳐지지 않은 레코드를 되돌리면 다시 실패해 DLT 로 돌아온다(DLT 발행기는 원본 헤더를 복사한다).
 * 재처리할 때마다 x-replay-count 헤더를 1 올리고, 상한(기본 3)에 닿은 레코드는 더 되돌리지 않고 로그만 남긴다.
 *
 * 왜 안전한가(= 재처리 실험으로 증명할 대상)
 *   - 이미 반영된 이벤트: 워터마크(last_applied_measured_at) 이하라 STALE 로 무시된다
 *   - XP 중복: 원장 유니크 제약 (user_id, source_type, source_id)
 *
 * 버린 대안
 *   - 원래 컨슈머가 DLT 를 같이 구독: 원인을 고치기 전에 다시 실패해 DLT 로 돌아가는 순환만 생긴다
 *   - 원본 토픽 오프셋 되감기: 정상 처리된 레코드까지 전부 다시 읽는다
 *   - eventId 로 골라 되돌리기: 그룹 오프셋은 "어디까지"만 기억하므로 고르지 않은 앞 레코드를 건너뛴 셈이 된다.
 *     고르는 대신 dryRun 으로 먼저 보고 전부 되돌린다
 */
@Slf4j
@Service
public class DltReplayService {

    public static final String REPLAY_COUNT_HEADER = "x-replay-count";
    static final String REPLAY_GROUP = "game-service-dlt-replay";
    static final String REPLAYED = "rpgym.kafka.dlt.replayed";
    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);

    private final Supplier<Consumer<String, String>> consumerFactory;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String dltTopic;
    private final String targetTopic;
    private final int maxReplays;
    private final long sendTimeoutMs;
    private final Counter replayedCounter;
    private final Counter skippedCounter;

    @Autowired
    public DltReplayService(KafkaProperties kafkaProperties,
                            KafkaTemplate<String, String> kafkaTemplate,
                            MeterRegistry meterRegistry,
                            @Value("${rpgym.kafka.health-events-topic}") String healthEventsTopic,
                            @Value("${rpgym.kafka.dlt-replay.max-replays:3}") int maxReplays) {
        this(() -> newConsumer(kafkaProperties), kafkaTemplate, meterRegistry,
                healthEventsTopic + ".DLT", healthEventsTopic, maxReplays, 5_000L);
    }

    DltReplayService(Supplier<Consumer<String, String>> consumerFactory,
                     KafkaTemplate<String, String> kafkaTemplate,
                     MeterRegistry meterRegistry,
                     String dltTopic,
                     String targetTopic,
                     int maxReplays,
                     long sendTimeoutMs) {
        this.consumerFactory = consumerFactory;
        this.kafkaTemplate = kafkaTemplate;
        this.dltTopic = dltTopic;
        this.targetTopic = targetTopic;
        this.maxReplays = maxReplays;
        this.sendTimeoutMs = sendTimeoutMs;
        this.replayedCounter = Counter.builder(REPLAYED).tag("result", "replayed")
                .description("DLT 에서 원래 토픽으로 되돌린 레코드 수").register(meterRegistry);
        this.skippedCounter = Counter.builder(REPLAYED).tag("result", "skipped_limit")
                .description("재처리 상한에 닿아 되돌리지 않은 레코드 수").register(meterRegistry);
    }

    public record Item(int partition, long offset, String key, int replayCount, String exception, String action) {
    }

    public record Report(boolean dryRun, int replayed, int skipped, List<Item> items) {
    }

    /**
     * @param dryRun     true 면 읽기만 하고 발행 · 커밋하지 않는다
     * @param maxRecords 이번 호출에서 다룰 최대 레코드 수
     */
    public Report replay(boolean dryRun, int maxRecords) {
        List<Item> items = new ArrayList<>();
        int replayed = 0;
        int skipped = 0;

        try (Consumer<String, String> consumer = consumerFactory.get()) {
            List<TopicPartition> partitions = partitionsOf(consumer);
            if (partitions.isEmpty()) {
                return new Report(dryRun, 0, 0, items);
            }
            consumer.assign(partitions);

            while (items.size() < maxRecords) {
                ConsumerRecords<String, String> records = consumer.poll(POLL_TIMEOUT);
                if (records.isEmpty()) {
                    break;
                }
                Map<TopicPartition, OffsetAndMetadata> done = new HashMap<>();
                boolean stop = false;

                for (ConsumerRecord<String, String> record : records) {
                    if (items.size() >= maxRecords) {
                        stop = true;
                        break;
                    }
                    int count = replayCount(record.headers());
                    String exception = headerString(record.headers(), "kafka_dlt-exception-fqcn");

                    if (count >= maxReplays) {
                        items.add(new Item(record.partition(), record.offset(), record.key(), count, exception, "SKIPPED_LIMIT"));
                        if (!dryRun) {
                            skipped++;
                            skippedCounter.increment();
                            log.error("재처리 상한({})에 닿아 되돌리지 않는다. partition={} offset={} key={}",
                                    maxReplays, record.partition(), record.offset(), record.key());
                            done.put(tp(record), new OffsetAndMetadata(record.offset() + 1));
                        }
                        continue;
                    }

                    if (dryRun) {
                        items.add(new Item(record.partition(), record.offset(), record.key(), count, exception, "WOULD_REPLAY"));
                        continue;
                    }

                    try {
                        ProducerRecord<String, String> out = new ProducerRecord<>(targetTopic, record.key(), record.value());
                        copyBusinessHeaders(record.headers(), out.headers());
                        out.headers().add(REPLAY_COUNT_HEADER, ByteBuffer.allocate(4).putInt(count + 1).array());
                        kafkaTemplate.send(out).get(sendTimeoutMs, TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        // 여기까지 성공한 것만 커밋하고 멈춘다. 이 레코드는 다음 호출 때 다시 시도된다
                        log.error("DLT 재발행 실패. 여기서 멈춘다. partition={} offset={}", record.partition(), record.offset(), e);
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        stop = true;
                        break;
                    }
                    replayed++;
                    replayedCounter.increment();
                    items.add(new Item(record.partition(), record.offset(), record.key(), count, exception, "REPLAYED"));
                    done.put(tp(record), new OffsetAndMetadata(record.offset() + 1));
                }

                if (!dryRun && !done.isEmpty()) {
                    consumer.commitSync(done);
                }
                if (stop || dryRun) {
                    // dryRun 은 커밋하지 않으므로 한 번 훑고 끝낸다(다시 poll 하면 같은 레코드가 또 나온다)
                    break;
                }
            }
        }

        log.info("DLT 재처리 끝. dryRun={} replayed={} skipped={} seen={}", dryRun, replayed, skipped, items.size());
        return new Report(dryRun, replayed, skipped, items);
    }

    private List<TopicPartition> partitionsOf(Consumer<String, String> consumer) {
        List<PartitionInfo> infos = consumer.partitionsFor(dltTopic);
        List<TopicPartition> result = new ArrayList<>();
        if (infos != null) {
            infos.forEach(i -> result.add(new TopicPartition(i.topic(), i.partition())));
        }
        return result;
    }

    // DLT 진단 헤더(kafka_dlt-*)는 원래 토픽으로 가져가지 않는다. 다시 실패하면 DLT 발행기가 새로 붙인다.
    private static void copyBusinessHeaders(Headers from, Headers to) {
        for (Header h : from) {
            if (h.key().startsWith("kafka_dlt-") || h.key().equals(REPLAY_COUNT_HEADER)) {
                continue;
            }
            to.add(h.key(), h.value());
        }
    }

    static int replayCount(Headers headers) {
        Header h = headers.lastHeader(REPLAY_COUNT_HEADER);
        if (h == null || h.value() == null || h.value().length != 4) {
            return 0;
        }
        return ByteBuffer.wrap(h.value()).getInt();
    }

    private static String headerString(Headers headers, String key) {
        Header h = headers.lastHeader(key);
        return h == null || h.value() == null ? null : new String(h.value(), java.nio.charset.StandardCharsets.UTF_8);
    }

    private static TopicPartition tp(ConsumerRecord<?, ?> r) {
        return new TopicPartition(r.topic(), r.partition());
    }

    private static Consumer<String, String> newConsumer(KafkaProperties kafkaProperties) {
        Map<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties(null));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, REPLAY_GROUP);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
