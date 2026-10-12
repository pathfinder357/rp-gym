package com.workoutdone.rpgym.game.quest.adapter.in.kafka;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerGroupLagMetricsTest {

    private static final TopicPartition P0 = new TopicPartition("health.events", 0);
    private static final TopicPartition P1 = new TopicPartition("health.events", 1);

    private final Map<TopicPartition, Long> committed = new HashMap<>();
    private final Map<TopicPartition, Long> ends = new HashMap<>();
    private final AtomicBoolean broken = new AtomicBoolean();

    private final ConsumerGroupLagMetrics.OffsetReader reader = new ConsumerGroupLagMetrics.OffsetReader() {
        @Override
        public Map<TopicPartition, Long> committed() {
            if (broken.get()) {
                throw new IllegalStateException("broker down");
            }
            return committed;
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Iterable<TopicPartition> partitions) {
            return ends;
        }
    };

    @DisplayName("R-23: 파티션별로 끝 오프셋 - 커밋 오프셋을 게이지로 낸다 (멈춘 파티션은 0 보다 크다)")
    @Test
    void exportsCommittedLagPerPartition() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ConsumerGroupLagMetrics metrics = new ConsumerGroupLagMetrics(registry, reader, 30_000);
        committed.put(P0, 100L);   // 실패한 레코드에서 멈춤: 커밋이 100 에 머물러 있다
        ends.put(P0, 106L);
        committed.put(P1, 50L);    // 다 처리함
        ends.put(P1, 50L);

        metrics.refresh();

        assertThat(registry.get(ConsumerGroupLagMetrics.LAG).tag("partition", "0").gauge().value()).isEqualTo(6.0);
        assertThat(registry.get(ConsumerGroupLagMetrics.LAG).tag("partition", "1").gauge().value()).isZero();
    }

    @DisplayName("R-23: 조회가 실패하면 실패 수를 올리고 마지막 값을 유지한다")
    @Test
    void failureKeepsLastValue() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ConsumerGroupLagMetrics metrics = new ConsumerGroupLagMetrics(registry, reader, 30_000);
        committed.put(P0, 100L);
        ends.put(P0, 103L);
        metrics.refresh();

        broken.set(true);
        metrics.refresh();

        assertThat(metrics.current(P0)).isEqualTo(3);
        assertThat(registry.get(ConsumerGroupLagMetrics.FAILED).counter().count()).isEqualTo(1.0);
    }

    @DisplayName("R-23: 커밋이 끝 오프셋보다 앞서 보이는 순간(조회 시점 차이)에도 음수를 내지 않는다")
    @Test
    void neverNegative() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ConsumerGroupLagMetrics metrics = new ConsumerGroupLagMetrics(registry, reader, 30_000);
        committed.put(P0, 10L);
        ends.put(P0, 9L);

        metrics.refresh();

        assertThat(metrics.current(P0)).isZero();
    }
}
