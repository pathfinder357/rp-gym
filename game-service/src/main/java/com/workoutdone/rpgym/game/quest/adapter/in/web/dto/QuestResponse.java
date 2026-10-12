package com.workoutdone.rpgym.game.quest.adapter.in.web.dto;

import com.workoutdone.rpgym.game.quest.application.QuestView;

import java.time.Instant;
import java.util.UUID;

/**
 * Quest 조회 응답.
 * metric/status를 String으로 내보낸다. enum을 그대로 노출하면 상수 이름을 바꾸는 순간
 * API 스펙이 바뀌게도ㅚㄴ다.
 */
public record QuestResponse(
        UUID questId,
        String title,
        String metric,
        int targetValue,
        int baselineValue,
        int currentValue,
        String status,
        int rewardXp,
        Instant expiredAt,
        // (R-18) 진행률의 기준 측정 시각. QuestView 주석 참고
        Instant progressAsOf
) {

    public static QuestResponse from(QuestView view) {
        return new QuestResponse(
                view.questId(),
                view.title(),
                view.metric().name(),
                view.targetValue(),
                view.baselineValue(),
                view.currentValue(),
                view.status().name(),
                view.rewardXp(),
                view.expiredAt(),
                view.progressAsOf()
        );
    }
}
