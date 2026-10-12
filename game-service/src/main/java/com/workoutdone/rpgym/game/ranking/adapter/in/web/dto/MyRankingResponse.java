package com.workoutdone.rpgym.game.ranking.adapter.in.web.dto;

import com.workoutdone.rpgym.game.ranking.application.MyRankingView;

import java.util.UUID;

public record MyRankingResponse(
        Long rank,
        long totalCount,
        Double percentile,
        UUID userId,
        int level,
        int totalXp,
        String tier,
        // (R-18) 랭킹 점수가 지갑 XP 를 따라잡았는가. MyRankingView 주석 참고
        boolean upToDate
) {

    public static MyRankingResponse from(MyRankingView view) {
        return new MyRankingResponse(
                view.rank(),
                view.totalCount(),
                view.percentile(),
                view.userId(),
                view.level(),
                view.totalXp(),
                view.tier().name(),
                view.upToDate()
        );
    }
}
