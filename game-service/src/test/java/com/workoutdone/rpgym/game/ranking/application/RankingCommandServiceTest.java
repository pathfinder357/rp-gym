package com.workoutdone.rpgym.game.ranking.application;

import com.workoutdone.rpgym.game.character.domain.XpClient;
import com.workoutdone.rpgym.game.ranking.FakeRankingStore;
import com.workoutdone.rpgym.game.ranking.domain.CharacterLevelWriter;
import com.workoutdone.rpgym.game.ranking.domain.RankingScore;
import com.workoutdone.rpgym.game.ranking.domain.RankingStore;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

public class RankingCommandServiceTest {

    private XpClient xpClient;
    private CharacterLevelWriter characterLevelWriter;
    private FakeRankingStore rankingStore;
    private RankingCommandService sut;
    private SimpleMeterRegistry meterRegistry;

    @BeforeEach
    void setUp(){
        xpClient = mock(XpClient.class);
        characterLevelWriter = mock(CharacterLevelWriter.class);
        rankingStore = new FakeRankingStore();
        meterRegistry = new SimpleMeterRegistry();
        sut = new RankingCommandService(xpClient, characterLevelWriter, rankingStore, new RankingMetrics(meterRegistry));
    }

    @DisplayName("같은 호출을 반복해도 점수가 같다 - ZADD 멱등성")
    @Test
    void idempotent(){
        UUID userId = UUID.randomUUID();
        given(xpClient.findTotalXp(userId)).willReturn(2450);

        sut.onXpChanged(userId);
        sut.onXpChanged(userId);
        sut.onXpChanged(userId);


        // 2450 XP → level 25 (1 + 2450/100)
        assertThat(rankingStore.findScore(userId))
                .contains(RankingScore.encode(25, 2450));
        assertThat(rankingStore.size()).isEqualTo(1);
        verify(characterLevelWriter, times(3)).upsertLevel(any(), anyInt());
    }

    @DisplayName("XP에서 계산한 레벨로 characters를 upsert한다.")
    @Test
    void upsertCharacterLevel(){
        UUID userId = UUID.randomUUID();
        given(xpClient.findTotalXp(userId)).willReturn(820);

        sut.onXpChanged(userId);

        verify(characterLevelWriter).upsertLevel(userId, 9);    // 1 + 820/100
    }

    @DisplayName("R-20: 먼저 읽은 낮은 점수가 늦게 써져도 랭킹 점수는 내려가지 않는다")
    @Test
    void staleLowerScoreDoesNotOverwrite(){
        UUID userId = UUID.randomUUID();
        // 스레드 B 가 먼저 900 을 반영했고, 스레드 A 가 그보다 앞서 읽어 둔 500 을 늦게 쓴다
        given(xpClient.findTotalXp(userId)).willReturn(900, 500);

        sut.onXpChanged(userId);
        sut.onXpChanged(userId);

        assertThat(rankingStore.findScore(userId)).contains(RankingScore.encode(10, 900));
    }

    @DisplayName("R-20: Redis 반영이 실패하면 예외를 삼키되 실패 카운터를 올린다")
    @Test
    void redisFailureIsCounted(){
        UUID userId = UUID.randomUUID();
        RankingStore broken = mock(RankingStore.class);
        doThrow(new IllegalStateException("redis down")).when(broken).save(any(), anyDouble());
        RankingCommandService withBroken =
                new RankingCommandService(xpClient, characterLevelWriter, broken, new RankingMetrics(meterRegistry));
        given(xpClient.findTotalXp(userId)).willReturn(300);

        withBroken.onXpChanged(userId);

        assertThat(meterRegistry.get("rpgym.ranking.update.failed").tag("stage", "redis").counter().count())
                .isEqualTo(1.0);
        verify(characterLevelWriter).upsertLevel(userId, 4);
    }
}
