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
