package com.workoutdone.rpgym.game.reconcile.application;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ReconciliationJobTest {

    private static final UUID STUCK = UUID.randomUUID();
    private static final UUID TRANSIENT = UUID.randomUUID();
    private static final Instant NOW = Instant.parse("2026-10-07T12:00:00Z");

    @DisplayName("R-15: 검사 결과를 check 별 게이지로 내보내고, 끝나면 마지막 성공 시각을 올린다")
    @Test
    void exportsGauges() {
        ReconciliationChecks checks = mock(ReconciliationChecks.class);
        given(checks.runSqlChecks()).willReturn(List.of(
                new ReconciliationChecks.Result("ledger_wallet", 2, List.of("u1", "u2")),
                new ReconciliationChecks.Result("quest_reward", 0, List.of()),
                new ReconciliationChecks.Result("party_counter", 0, List.of()),
                new ReconciliationChecks.Result("party_reward", 0, List.of())));
        given(checks.findRankingMismatches()).willReturn(Set.of(STUCK, TRANSIENT), Set.of(STUCK));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        ReconciliationJob job = new ReconciliationJob(checks, registry, Clock.fixed(NOW, ZoneOffset.UTC));
        job.run();
        // 첫 대조: 랭킹은 아직 확정하지 않는다(반영이 늦은 것일 수 있다)
        assertThat(registry.get(ReconciliationJob.MISMATCH).tag("check", "ranking").gauge().value()).isZero();

        job.run();
        // 두 번째: 연속으로 어긋난 STUCK 만 센다. TRANSIENT 는 그 사이 맞춰졌다
        assertThat(registry.get(ReconciliationJob.MISMATCH).tag("check", "ledger_wallet").gauge().value()).isEqualTo(2.0);
        assertThat(registry.get(ReconciliationJob.MISMATCH).tag("check", "ranking").gauge().value()).isEqualTo(1.0);
        assertThat(registry.get(ReconciliationJob.MISMATCH).tag("check", "party_reward").gauge().value()).isZero();
        assertThat(registry.get(ReconciliationJob.LAST_SUCCESS).gauge().value()).isEqualTo(NOW.getEpochSecond());
    }

    @DisplayName("R-15: 대조가 실패하면 예외를 삼키고, 마지막 성공 시각은 그대로 둔다(멈춤이 드러나게)")
    @Test
    void failureKeepsLastSuccess() {
        ReconciliationChecks checks = mock(ReconciliationChecks.class);
        given(checks.runSqlChecks()).willThrow(new IllegalStateException("db down"));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        ReconciliationJob job = new ReconciliationJob(checks, registry, Clock.fixed(NOW, ZoneOffset.UTC));
        job.run();

        assertThat(registry.get(ReconciliationJob.LAST_SUCCESS).gauge().value()).isZero();
    }

    @DisplayName("R-21: Redis(랭킹) 검사가 실패해도 SQL 검사 게이지와 마지막 성공 시각은 갱신된다")
    @Test
    void rankingFailureDoesNotStopSqlChecks() {
        ReconciliationChecks checks = mock(ReconciliationChecks.class);
        given(checks.runSqlChecks()).willReturn(List.of(
                new ReconciliationChecks.Result("ledger_wallet", 3, List.of("u1", "u2", "u3")),
                new ReconciliationChecks.Result("quest_reward", 0, List.of()),
                new ReconciliationChecks.Result("party_counter", 0, List.of()),
                new ReconciliationChecks.Result("party_reward", 0, List.of())));
        given(checks.findRankingMismatches()).willThrow(new IllegalStateException("redis down"));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        ReconciliationJob job = new ReconciliationJob(checks, registry, Clock.fixed(NOW, ZoneOffset.UTC));
        job.run();

        assertThat(registry.get(ReconciliationJob.MISMATCH).tag("check", "ledger_wallet").gauge().value()).isEqualTo(3.0);
        assertThat(registry.get(ReconciliationJob.LAST_SUCCESS).gauge().value()).isEqualTo(NOW.getEpochSecond());
        assertThat(registry.get(ReconciliationJob.FAILED).tag("stage", "ranking").counter().count()).isEqualTo(1.0);
        assertThat(registry.get(ReconciliationJob.FAILED).tag("stage", "sql").counter().count()).isZero();
    }

    @DisplayName("R-21: SQL 검사가 실패해도 랭킹 검사는 돈다. 마지막 성공 시각은 그대로")
    @Test
    void sqlFailureStillRunsRanking() {
        ReconciliationChecks checks = mock(ReconciliationChecks.class);
        given(checks.runSqlChecks()).willThrow(new IllegalStateException("db down"));
        given(checks.findRankingMismatches()).willReturn(Set.of(STUCK));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        ReconciliationJob job = new ReconciliationJob(checks, registry, Clock.fixed(NOW, ZoneOffset.UTC));
        job.run();
        job.run();

        verify(checks, org.mockito.Mockito.times(2)).findRankingMismatches();
        assertThat(registry.get(ReconciliationJob.MISMATCH).tag("check", "ranking").gauge().value()).isEqualTo(1.0);
        assertThat(registry.get(ReconciliationJob.LAST_SUCCESS).gauge().value()).isZero();
        assertThat(registry.get(ReconciliationJob.FAILED).tag("stage", "sql").counter().count()).isEqualTo(2.0);
    }

    @DisplayName("R-21: 랭킹 검사 실패를 사이에 둔 두 관측은 '연속' 으로 치지 않는다")
    @Test
    void rankingFailureResetsConsecutiveConfirmation() {
        ReconciliationChecks checks = mock(ReconciliationChecks.class);
        given(checks.runSqlChecks()).willReturn(List.of());
        given(checks.findRankingMismatches())
                .willReturn(Set.of(STUCK))
                .willThrow(new IllegalStateException("redis down"))
                .willReturn(Set.of(STUCK), Set.of(STUCK));
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        ReconciliationJob job = new ReconciliationJob(checks, registry, Clock.fixed(NOW, ZoneOffset.UTC));
        job.run();   // 관측 1
        job.run();   // 실패
        job.run();   // 복구 후 첫 관측: 실패 전 관측과 이어 붙이지 않으므로 아직 0
        assertThat(registry.get(ReconciliationJob.MISMATCH).tag("check", "ranking").gauge().value()).isZero();
        job.run();   // 복구 후 두 번째 관측: 확정
        assertThat(registry.get(ReconciliationJob.MISMATCH).tag("check", "ranking").gauge().value()).isEqualTo(1.0);
    }
}
