package com.workoutdone.rpgym.game.quest.adapter.in.kafka;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class ConsumerLatencyMetricsTest {

    private static final Instant NOW = Instant.parse("2026-10-07T12:00:00Z");

    @DisplayName("R-16: 파이프라인 지연은 레코드 timestamp 기준, 화면 지연은 measuredAt 기준으로 기록한다")
    @Test
    void recordsBothLatencies() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ConsumerLatencyMetrics metrics = new ConsumerLatencyMetrics(registry, Clock.fixed(NOW, ZoneOffset.UTC));

        metrics.record(NOW.minusSeconds(3).toEpochMilli(), Optional.of(NOW.minusSeconds(600)));

        assertThat(registry.get(ConsumerLatencyMetrics.PIPELINE).timer().totalTime(TimeUnit.SECONDS)).isEqualTo(3.0);
        assertThat(registry.get(ConsumerLatencyMetrics.FRESHNESS).timer().totalTime(TimeUnit.SECONDS)).isEqualTo(600.0);
    }

    @DisplayName("R-16: measuredAt 이 없는 이벤트는 화면 지연을 남기지 않고, 시계가 어긋난 음수는 0으로 기록한다")
    @Test
    void skipsFreshnessAndClampsNegative() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        ConsumerLatencyMetrics metrics = new ConsumerLatencyMetrics(registry, Clock.fixed(NOW, ZoneOffset.UTC));

        metrics.record(NOW.plusSeconds(2).toEpochMilli(), Optional.empty());

        assertThat(registry.get(ConsumerLatencyMetrics.PIPELINE).timer().count()).isEqualTo(1);
        assertThat(registry.get(ConsumerLatencyMetrics.PIPELINE).timer().totalTime(TimeUnit.SECONDS)).isZero();
        assertThat(registry.get(ConsumerLatencyMetrics.FRESHNESS).timer().count()).isZero();
    }
}
