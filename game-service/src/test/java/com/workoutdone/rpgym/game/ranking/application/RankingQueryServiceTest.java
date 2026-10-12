package com.workoutdone.rpgym.game.ranking.application;

import com.workoutdone.rpgym.game.character.domain.XpClient;
import com.workoutdone.rpgym.game.ranking.domain.RankingScore;
import com.workoutdone.rpgym.game.character.domain.CharacterTier;
import com.workoutdone.rpgym.game.ranking.FakeRankingStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RankingQueryServiceTest {

    private FakeRankingStore rankingStore;
    private int stubbedXp;
    private RankingQueryService sut;

    @BeforeEach
    void setUp() {
        rankingStore = new FakeRankingStore();
        stubbedXp = 0;
        XpClient xpClient = userId -> stubbedXp;
        sut = new RankingQueryService(rankingStore, xpClient);
    }

    @DisplayName("동점자는 같은 순위를 받고 다음 순위는 건너뛴다")
    @Test
    void tiedMembersShareRankAndSkipNext() {
        // score 900, 800, 800, 700 → rank 1, 2, 2, 4
        putAll(900, 800, 800, 700);

        List<RankingEntryView> content = sut.getRankings(0, 10).content();

        assertThat(content).extracting(RankingEntryView::rank)
                .containsExactly(1L, 2L, 2L, 4L);
    }

    @DisplayName("페이지가 동점 그룹 중간에서 시작해도 그 뒤 사람의 순위가 밀리지 않는다")
    @Test
    void rankAfterTieGroupSurvivesPageBoundary() {
        // score 900, 800, 800, 700 → rank 1, 2, 2, 4
        // page=1,size=2 는 동점 그룹(800,800)의 두 번째에서 시작한다.
        putAll(900, 800, 800, 700);

        long fromFullPage = sut.getRankings(0, 10).content().get(3).rank();
        long fromSplitPage = sut.getRankings(1, 2).content().get(1).rank();

        assertThat(fromSplitPage)
                .isEqualTo(fromFullPage)
                .isEqualTo(4L);
    }

    @DisplayName("페이지가 갈려도 동점자 순위가 유지된다")
    @Test
    void rankSurvivesPageBoundary() {
        // 20번째와 21번째가 동점이 되도록 배치한다
        List<Integer> scores = new ArrayList<>();
        for (int i = 0; i < 19; i++) {
            scores.add(1000 - i);        // 1000 ~ 982 (19명, 전부 다른 점수)
        }
        scores.add(500);                  // 20번째
        scores.add(500);                  // 21번째 — 20번째와 동점
        scores.add(400);
        putAll(scores.stream().mapToInt(Integer::intValue).toArray());

        long lastOfPage1 = sut.getRankings(0, 20).content().get(19).rank();
        long firstOfPage2 = sut.getRankings(1, 20).content().get(0).rank();

        assertThat(firstOfPage2).isEqualTo(lastOfPage1);
    }

    @DisplayName("level 과 totalXp 와 tier 를 score 에서 복원한다")
    @Test
    void decodesFromScore() {
        UUID userId = UUID.randomUUID();
        rankingStore.save(userId, 12_000_002_450.0);

        RankingEntryView entry = sut.getRankings(0, 10).content().get(0);

        assertThat(entry.level()).isEqualTo(12);
        assertThat(entry.totalXp()).isEqualTo(2450);
        assertThat(entry.tier()).isEqualTo(CharacterTier.SILVER);
    }

    @DisplayName("랭킹이 비어 있어도 404 가 아니라 빈 목록을 돌려준다")
    @Test
    void emptyRankingIsNotAnError() {
        RankingPageView view = sut.getRankings(0, 20);

        assertThat(view.content()).isEmpty();
        assertThat(view.totalElements()).isZero();
    }

    @DisplayName("범위를 벗어난 페이지는 빈 목록이다")
    @Test
    void outOfRangePageIsEmpty() {
        putAll(900, 800);

        assertThat(sut.getRankings(5, 20).content()).isEmpty();
    }

    @DisplayName("size 가 허용 범위를 벗어나면 예외를 던진다")
    @Test
    void rejectsInvalidSize() {
        assertThatThrownBy(() -> sut.getRankings(0, 0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.getRankings(0, 101))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> sut.getRankings(-1, 20))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @DisplayName("내 랭킹은 나보다 높은 사람 수 + 1 이다 — 동점자와 같은 순위")
    @Test
    void myRankMatchesListRank() {
        putAll(900, 800, 800, 700);
        UUID tied = rankingStore.findPage(1, 1).get(0).userId();

        MyRankingView view = sut.getMyRanking(tied);

        assertThat(view.rank()).isEqualTo(2L);
        assertThat(view.totalCount()).isEqualTo(4L);
        assertThat(view.percentile()).isEqualTo(50.0);   // 2 / 4 * 100
    }

    @DisplayName("R-18: 랭킹 점수가 지갑 XP 보다 뒤처져 있으면 upToDate=false, 따라잡았으면 true")
    @Test
    void reportsWhetherRankingCaughtUpWithWallet() {
        UUID me = UUID.randomUUID();
        rankingStore.save(me, RankingScore.encode(3, 250));

        stubbedXp = 300;   // 지갑은 이미 300 (랭킹 반영 전)
        assertThat(sut.getMyRanking(me).upToDate()).isFalse();
        assertThat(sut.getMyRanking(me).totalXp()).isEqualTo(250);

        stubbedXp = 250;
        assertThat(sut.getMyRanking(me).upToDate()).isTrue();
    }

    @DisplayName("R-18: XP 가 있는데 랭킹에 없으면 아직 반영 전(upToDate=false), XP 가 0 이면 정상(true)")
    @Test
    void notRankedUpToDateFlag() {
        stubbedXp = 0;
        assertThat(sut.getMyRanking(UUID.randomUUID()).upToDate()).isTrue();
        stubbedXp = 120;
        assertThat(sut.getMyRanking(UUID.randomUUID()).upToDate()).isFalse();
    }

    @DisplayName("랭킹에 없는 사용자는 404 가 아니라 기본값을 받는다")
    @Test
    void notRankedUserGetsDefaults() {
        putAll(900);

        MyRankingView view = sut.getMyRanking(UUID.randomUUID());

        assertThat(view.rank()).isNull();
        assertThat(view.percentile()).isNull();
        assertThat(view.level()).isEqualTo(1);
        assertThat(view.totalXp()).isZero();
        assertThat(view.tier()).isEqualTo(CharacterTier.BRONZE);
        assertThat(view.totalCount()).isEqualTo(1L);
    }

    @DisplayName("ZSET 에 없어도 XP 원본 값을 그대로 돌려준다 — /characters/me 와 어긋나지 않는다")
    @Test
    void notRankedUserStillReportsRealXp() {
        putAll(900);
        stubbedXp = 250;

        MyRankingView view = sut.getMyRanking(UUID.randomUUID());

        assertThat(view.rank()).isNull();          // 랭킹 집계에는 아직 없다
        assertThat(view.level()).isEqualTo(3);     // 그래도 XP 는 보인다
        assertThat(view.totalXp()).isEqualTo(250);
        assertThat(view.tier()).isEqualTo(CharacterTier.BRONZE);
    }

    private void putAll(int... levels) {
        for (int level : levels) {
            rankingStore.save(UUID.randomUUID(), RankingScore.encode(level, 0));
        }
    }
}