package com.workoutdone.rpgym.game.ranking.adapter.out.batch;

import com.workoutdone.rpgym.game.character.adapter.out.persistence.CharacterRepository;
import com.workoutdone.rpgym.game.character.adapter.out.persistence.CharacterRepository.RankingSourceRow;
import com.workoutdone.rpgym.game.ranking.FakeRankingStore;
import com.workoutdone.rpgym.game.ranking.domain.RankingScore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

class RankingRebuildJobTest {

    @DisplayName("R-20: 1,000명 단위 키셋 페이지로 끝까지 읽고, 레벨은 XP 에서 계산한다")
    @Test
    void rebuildsAllPages() {
        CharacterRepository repository = mock(CharacterRepository.class);
        FakeRankingStore store = new FakeRankingStore();

        List<RankingSourceRow> first = new ArrayList<>();
        for (int i = 0; i < RankingRebuildJob.BATCH_SIZE; i++) {
            first.add(row(new UUID(0, i + 1), i));
        }
        UUID lastOfFirst = first.get(first.size() - 1).getUserId();
        RankingSourceRow noWallet = row(new UUID(1, 1), null);

        given(repository.findRankingSource(any(), anyInt())).willReturn(first);
        given(repository.findRankingSource(eq(lastOfFirst), anyInt())).willReturn(List.of(noWallet));

        long rebuilt = new RankingRebuildJob(repository, store).rebuild();

        assertThat(rebuilt).isEqualTo(RankingRebuildJob.BATCH_SIZE + 1L);
        assertThat(store.findScore(new UUID(0, 251))).contains(RankingScore.encode(3, 250));
        assertThat(store.findScore(noWallet.getUserId())).contains(RankingScore.encode(1, 0));
    }

    @DisplayName("R-20: 재구축이 읽어 둔 옛 값은 그 사이 들어온 더 큰 점수를 덮지 않는다")
    @Test
    void rebuildDoesNotLowerNewerScore() {
        CharacterRepository repository = mock(CharacterRepository.class);
        FakeRankingStore store = new FakeRankingStore();
        UUID userId = UUID.randomUUID();
        store.save(userId, RankingScore.encode(6, 520)); // 재구축 도중 실시간 경로가 먼저 반영

        given(repository.findRankingSource(any(), anyInt())).willReturn(List.of(row(userId, 480)));

        new RankingRebuildJob(repository, store).rebuild();

        assertThat(store.findScore(userId)).contains(RankingScore.encode(6, 520));
    }

    private static RankingSourceRow row(UUID userId, Integer xp) {
        return new RankingSourceRow() {
            public UUID getUserId() { return userId; }
            public Integer getTotalXp() { return xp; }
        };
    }
}
