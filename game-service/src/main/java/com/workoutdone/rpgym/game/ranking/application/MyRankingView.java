package com.workoutdone.rpgym.game.ranking.application;

import com.workoutdone.rpgym.game.character.domain.CharacterTier;
import com.workoutdone.rpgym.game.character.domain.LevelPolicy;
import com.workoutdone.rpgym.game.ranking.domain.RankingScore;

import java.util.UUID;

//내 랭킹 결과
public record MyRankingView(
        Long rank,
        long totalCount,
        Double percentile,
        UUID userId,
        int level,
        int totalXp,
        CharacterTier tier,
        // 읽기 모델이라 잠깐 뒤처질 수 있다. false 면 캐릭터 화면(지갑 기준)과 랭킹 화면의 XP 가 다르다는 뜻이고,
        boolean upToDate
) {
    public static MyRankingView of(UUID userId, long rank, long totalCount, double score, int walletXp){
        int level = RankingScore.decodeLevel(score);
        return new MyRankingView(
                rank,
                totalCount,
                percentileOf(rank, totalCount),
                userId,
                level,
                RankingScore.decodeXp(score),
                CharacterTier.of(level),
                RankingScore.decodeXp(score) >= walletXp
        );
    }

    /** ZSET 에 아직 없는 사용자. 404 가 아니라 기본값 200 으로 응답한다. */
    public static MyRankingView notRanked(UUID userId, long totalCount, int totalXp){
        int level = LevelPolicy.levelOf(totalXp);
        return new MyRankingView(
                null,
                totalCount,
                null,
                userId,
                level,
                totalXp,
                CharacterTier.of(level),
                // XP 가 0 이면 랭킹에 없는 게 맞다. XP 가 있는데 없다면 아직 반영 전이다
                totalXp == 0
        );
    }

    // 상위 백분위(%) 값이 작을수록 상위다. 소수 첫째 자리에서 반올림함.
    private static Double percentileOf(long rank, long totalCount){
        if (totalCount <= 0){
            return null;
        }
        return Math.round(rank * 1000.0 / totalCount) / 10.0;
    }
}
