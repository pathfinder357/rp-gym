package com.workoutdone.rpgym.game.quest.application;

import com.workoutdone.rpgym.game.quest.domain.Metric;
import com.workoutdone.rpgym.game.quest.domain.QuestStatus;
import com.workoutdone.rpgym.game.quest.domain.aggregate.Quest;

import java.time.Instant;
import java.util.UUID;

/**
 * 조회 경계 타입.
 * 도메인 Quest를 그대로 웹으로 넘기지 않는다.
 * 도메인 엔티티도 어댑터로 나가지 않아야한다. 나가면 컬럼 하나 바꿀 때마다 응답 스펙에 따라 바뀐다.
 * currentValue는 quest.progress()가 계산한다 -> 달성분 = last_cumulative_val - baseline_val.
 * 첫 이벤트 전이면 0이다.
 *
 * (R-18) progressAsOf -- 이 진행률이 "언제 측정한 값" 기준인가. 마지막으로 반영한 스냅샷의 measuredAt,
 * 아직 반영한 게 없으면 baseline 측정 시각이다. 이벤트는 비동기로 반영되므로 화면 값은 항상 조금 낡아 있다.
 * 낡은 값을 최신처럼 보여주지 않으려면 그 기준 시각을 같이 내려야 한다. 스파이크 때(큐 대기 p95 수십 초)
 * 클라이언트는 이 값으로 "n분 전 기준"을 표시할 수 있다.
 */
public record QuestView(
        UUID questId,
        String title,
        Metric metric,
        int targetValue,
        int baselineValue,
        int currentValue,
        QuestStatus status,
        int rewardXp,
        Instant expiredAt,
        Instant progressAsOf
) {

    public static QuestView from(Quest quest) {
        return new QuestView(
                quest.getQuestId(),
                quest.getTitle(),
                quest.getMetric(),
                quest.getTargetVal(),
                quest.getBaselineVal(),
                quest.progress(),
                quest.getStatus(),
                quest.getRewardXp(),
                quest.getExpiredAt(),
                quest.getLastAppliedMeasuredAt() != null
                        ? quest.getLastAppliedMeasuredAt()
                        : quest.getBaselineMeasuredAt()
        );
    }
}
