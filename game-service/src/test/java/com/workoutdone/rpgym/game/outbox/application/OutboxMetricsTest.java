package com.workoutdone.rpgym.game.outbox.application;

import com.workoutdone.rpgym.game.outbox.domain.repo.OutboxEventRepository;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OutboxMetricsTest {

    private final OutboxEventRepository repository = mock(OutboxEventRepository.class);
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();

    private double oldestPendingAge() {
        new OutboxMetrics(repository, registry);
        return registry.get(OutboxMetrics.OLDEST_PENDING_AGE).gauge().value();
    }

    @Test
    @DisplayName("가장 오래된 PENDING이 90초 전에 적재됐으면 약 90초를 보고한다")
    void 가장_오래된_PENDING의_나이를_보고한다() {
        when(repository.findOldestPendingCreatedAt())
                .thenReturn(Optional.of(LocalDateTime.now().minusSeconds(90)));

        assertThat(oldestPendingAge()).isBetween(90.0, 95.0);
    }

    @Test
    @DisplayName("PENDING이 없으면 0이다 — 새벽처럼 트래픽이 없어도 장애로 보이지 않는다")
    void PENDING이_없으면_0이다() {
        when(repository.findOldestPendingCreatedAt()).thenReturn(Optional.empty());

        assertThat(oldestPendingAge()).isZero();
    }

    @Test
    @DisplayName("수집할 때마다 DB를 다시 읽는다 — 발행기가 멈춰도 값이 멈추지 않는다")
    void 수집할_때마다_다시_읽는다() {
        when(repository.findOldestPendingCreatedAt())
                .thenReturn(Optional.empty())
                .thenReturn(Optional.of(LocalDateTime.now().minusSeconds(120)));

        new OutboxMetrics(repository, registry);
        var gauge = registry.get(OutboxMetrics.OLDEST_PENDING_AGE).gauge();

        assertThat(gauge.value()).isZero();
        assertThat(gauge.value()).isBetween(120.0, 125.0);
    }
}
