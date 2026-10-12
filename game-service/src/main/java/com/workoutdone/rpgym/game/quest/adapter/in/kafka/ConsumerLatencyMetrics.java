package com.workoutdone.rpgym.game.quest.adapter.in.kafka;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/**
 * (R-16) "사용자가 얼마나 기다리나"를 상시 지표로 만든다.
 *
 * 예전에는 컨슈머 lag(줄 선 메시지 수)과 Outbox 최장 대기만 보였다. lag 은 개수라
 * "정각에 몇 초 늦나"로 바로 읽히지 않고, 스파이크 때 잰 큐 대기 p95(23.7~45초)는 실험 스크립트로만 나왔다.
 *
 * Timer 두 개
 *   rpgym.game.consumer.pipeline.latency = 처리 완료 - Kafka 레코드 timestamp(Health 가 보낸 시각)
 *       Game 이 책임지는 구간(브로커 대기 + 큐 대기 + 처리). 알림은 이 값의 p95 에만 건다.
 *   rpgym.game.consumer.freshness       = 처리 완료 - 스냅샷 measuredAt(사용자 기기가 잰 시각)
 *       30분 동기화 주기와 Health 쪽 지연까지 포함한 "화면 값이 얼마나 낡았나". 관찰용, 알림 없음.
 *       HEALTH_ACTIVITY_SYNCED 만 해당한다(나머지 타입은 measuredAt 이 없다).
 *
 * 레코드 timestamp 는 기본 설정(CreateTime)에서 프로듀서가 찍은 시각이다. Health 서버 시계를 믿는 셈이라
 * 두 서버 시계가 어긋나면 그만큼 오차가 생긴다(같은 EC2 에서는 무시할 수준). 음수는 0 으로 기록한다.
 * 히스토그램을 내보내 Prometheus 에서 histogram_quantile 로 p95 를 계산한다. userId 같은 라벨은 달지 않는다.
 */
@Component
public class ConsumerLatencyMetrics {

    static final String PIPELINE = "rpgym.game.consumer.pipeline.latency";
    static final String FRESHNESS = "rpgym.game.consumer.freshness";

    private final Timer pipeline;
    private final Timer freshness;
    private final Clock clock;

    public ConsumerLatencyMetrics(MeterRegistry meterRegistry, Clock clock) {
        this.clock = clock;
        this.pipeline = Timer.builder(PIPELINE)
                .description("Health 가 발행한 시각부터 Game 이 처리를 끝낸 시각까지")
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(10))
                .maximumExpectedValue(Duration.ofMinutes(30))
                .register(meterRegistry);
        this.freshness = Timer.builder(FRESHNESS)
                .description("기기가 측정한 시각부터 Game 이 진행률에 반영한 시각까지")
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofSeconds(1))
                .maximumExpectedValue(Duration.ofHours(2))
                .register(meterRegistry);
    }

    public void record(long recordTimestampMillis, Optional<Instant> measuredAt) {
        Instant now = clock.instant();
        if (recordTimestampMillis > 0) {
            pipeline.record(nonNegative(Duration.between(Instant.ofEpochMilli(recordTimestampMillis), now)));
        }
        measuredAt.ifPresent(m -> freshness.record(nonNegative(Duration.between(m, now))));
    }

    private static Duration nonNegative(Duration d) {
        return d.isNegative() ? Duration.ZERO : d;
    }
}
