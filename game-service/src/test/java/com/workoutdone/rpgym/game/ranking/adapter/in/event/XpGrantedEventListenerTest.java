package com.workoutdone.rpgym.game.ranking.adapter.in.event;

import com.workoutdone.rpgym.game.ranking.application.RankingMetrics;
import com.workoutdone.rpgym.game.ranking.application.RankingService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import com.workoutdone.rpgym.game.xp.application.XpGranted;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.BDDMockito.willThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class XpGrantedEventListenerTest {

    private RankingService rankingService;
    private XpGrantedEventListener sut;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp() {
        rankingService = mock(RankingService.class);
        meterRegistry = new SimpleMeterRegistry();
        sut = new XpGrantedEventListener(rankingService, new RankingMetrics(meterRegistry));
    }

    @DisplayName("XpGranted 를 받으면 해당 사용자의 랭킹을 갱신한다")
    @Test
    void delegatesToRankingService() {
        UUID userId = UUID.randomUUID();

        sut.on(new XpGranted(userId));

        verify(rankingService).onXpChanged(userId);
    }

    @DisplayName("랭킹 갱신이 실패해도 예외를 밖으로 던지지 않는다 — XP 지급과 컨슈머를 보호한다")
    @Test
    void swallowsFailure() {
        UUID userId = UUID.randomUUID();
        // Redis 장애를 가정한다. AFTER_COMMIT 리스너의 예외는 커밋 호출자에게 전파되므로,
        // 여기서 새어 나가면 Kafka offset 이 안 잡혀 파티션이 멈춘다.
        willThrow(new RuntimeException("redis down"))
                .given(rankingService).onXpChanged(userId);

        assertThatCode(() -> sut.on(new XpGranted(userId)))
                .doesNotThrowAnyException();
        // R-20: 삼키되 세어 둔다
        org.assertj.core.api.Assertions.assertThat(
                meterRegistry.get("rpgym.ranking.update.failed").tag("stage", "listener").counter().count())
                .isEqualTo(1.0);
    }
}
