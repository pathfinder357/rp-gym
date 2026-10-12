package com.workoutdone.rpgym.game.dlt.application;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class DltReplayServiceTest {

    private static final String DLT = "health.events.DLT";
    private static final String TARGET = "health.events";
    private static final TopicPartition P0 = new TopicPartition(DLT, 0);

    private MockConsumer<String, String> consumer;
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private SimpleMeterRegistry registry;
    private DltReplayService sut;

    @BeforeEach
    void setUp() {
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            // 서비스가 try-with-resources 로 닫는다. 테스트가 커밋 위치를 확인할 수 있게 닫힘만 막는다
            @Override
            public synchronized void close() {
            }
        };
        consumer.updatePartitions(DLT, List.of(new PartitionInfo(DLT, 0, Node.noNode(), new Node[0], new Node[0])));
        consumer.updateBeginningOffsets(Map.of(P0, 0L));
        registry = new SimpleMeterRegistry();
        sut = new DltReplayService(() -> consumer, kafkaTemplate, registry, DLT, TARGET, 3, 1_000L);
    }

    @DisplayName("R-19: 같은 key · 같은 value 로 원래 토픽에 다시 보내고, 재처리 횟수 헤더를 1 올리고, DLT 진단 헤더는 떼고, 보낸 데까지 커밋한다")
    @Test
    void replaysWithSameKeyAndValue() {
        givenSendSucceeds();
        RecordHeaders headers = new RecordHeaders();
        headers.add("eventType", "HEALTH_ACTIVITY_SYNCED".getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-exception-fqcn", "java.lang.NullPointerException".getBytes(StandardCharsets.UTF_8));
        addRecord(0, "user-1", "{\"eventId\":\"e1\"}", headers);
        addRecord(1, "user-2", "{\"eventId\":\"e2\"}", new RecordHeaders());

        DltReplayService.Report report = sut.replay(false, 100);

        assertThat(report.replayed()).isEqualTo(2);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> sent = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate, times(2)).send(sent.capture());
        ProducerRecord<String, String> first = sent.getAllValues().get(0);
        assertThat(first.topic()).isEqualTo(TARGET);
        assertThat(first.key()).isEqualTo("user-1");
        assertThat(first.value()).isEqualTo("{\"eventId\":\"e1\"}");
        assertThat(DltReplayService.replayCount(first.headers())).isEqualTo(1);
        assertThat(first.headers().lastHeader("eventType")).isNotNull();
        assertThat(first.headers().lastHeader("kafka_dlt-exception-fqcn")).isNull();
        assertThat(consumer.committed(java.util.Set.of(P0)).get(P0).offset()).isEqualTo(2L);
        assertThat(registry.get(DltReplayService.REPLAYED).tag("result", "replayed").counter().count()).isEqualTo(2.0);
    }

    @DisplayName("R-19: 재처리 상한에 닿은 레코드는 되돌리지 않는다 -- DLT ↔ 원본 무한 순환 방지")
    @Test
    void skipsRecordsAtLimit() {
        RecordHeaders headers = new RecordHeaders();
        headers.add(DltReplayService.REPLAY_COUNT_HEADER, ByteBuffer.allocate(4).putInt(3).array());
        addRecord(0, "user-1", "{}", headers);

        DltReplayService.Report report = sut.replay(false, 100);

        assertThat(report.skipped()).isEqualTo(1);
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
        assertThat(consumer.committed(java.util.Set.of(P0)).get(P0).offset()).isEqualTo(1L);
    }

    @DisplayName("R-19: 발행이 실패하면 그 앞까지만 커밋하고 멈춘다 -- 실패한 레코드는 다음 호출에서 다시 시도")
    @Test
    void stopsOnSendFailure() {
        CompletableFuture<SendResult<String, String>> ok = CompletableFuture.completedFuture(null);
        CompletableFuture<SendResult<String, String>> fail = CompletableFuture.failedFuture(new IllegalStateException("broker down"));
        given(kafkaTemplate.send(any(ProducerRecord.class))).willReturn(ok, fail);
        addRecord(0, "user-1", "{}", new RecordHeaders());
        addRecord(1, "user-2", "{}", new RecordHeaders());
        addRecord(2, "user-3", "{}", new RecordHeaders());

        DltReplayService.Report report = sut.replay(false, 100);

        assertThat(report.replayed()).isEqualTo(1);
        assertThat(consumer.committed(java.util.Set.of(P0)).get(P0).offset()).isEqualTo(1L);
    }

    @DisplayName("R-19: dryRun 은 보여주기만 한다 -- 발행도 커밋도 하지 않는다")
    @Test
    void dryRunDoesNothing() {
        addRecord(0, "user-1", "{}", new RecordHeaders());

        DltReplayService.Report report = sut.replay(true, 100);

        assertThat(report.items()).extracting(DltReplayService.Item::action).containsExactly("WOULD_REPLAY");
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
        assertThat(Optional.ofNullable(consumer.committed(java.util.Set.of(P0)).get(P0))).isEmpty();
    }

    private void givenSendSucceeds() {
        given(kafkaTemplate.send(any(ProducerRecord.class))).willReturn(CompletableFuture.completedFuture(null));
    }

    private void addRecord(long offset, String key, String value, RecordHeaders headers) {
        consumer.schedulePollTask(() -> consumer.addRecord(new ConsumerRecord<>(DLT, 0, offset, 0L,
                TimestampType.CREATE_TIME, 0, 0, key, value, headers, Optional.empty())));
    }
}
