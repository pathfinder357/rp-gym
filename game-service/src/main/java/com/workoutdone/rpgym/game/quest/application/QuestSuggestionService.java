package com.workoutdone.rpgym.game.quest.application;

import com.workoutdone.rpgym.game.outbox.application.OutboxRecorder;
import com.workoutdone.rpgym.game.outbox.domain.AggregateType;
import com.workoutdone.rpgym.game.outbox.domain.OutboxEventType;
import com.workoutdone.rpgym.game.quest.application.payload.QuestToNotification;
import com.workoutdone.rpgym.game.quest.domain.Metric;
import com.workoutdone.rpgym.game.quest.domain.aggregate.QuestSuggestion;
import com.workoutdone.rpgym.game.quest.domain.repo.QuestSuggestionRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

// Health 가 보낸 제안을 받아서 보관하고, 알림용 이벤트를 아웃박스에 적재한다.
//
// 예전에는 이 자리에서 곧바로 퀘스트를 만들었다. 유저에게 물어보지 않고 자동으로 수락한 셈이다.
// 원래 기획은 Slack 카드로 수락과 거절을 고르게 하는 것이었는데 Slack 연동이 미뤄지면서
// 수락 단계가 통째로 빠져 있었다. 이제 그 단계를 되살린다.
//
// 여기서 하는 검사의 기준은 하나다. "이 제안을 사람에게 보여줄 수 있는가."
// "이 제안으로 지금 퀘스트를 만들 수 있는가" 는 수락하는 순간에 물어야 맞는 질문이라
// QuestSuggestionAcceptService 로 옮겼다.
// 예를 들어 이미 진행 중인 퀘스트가 있는지는 여기서 답해봐야 30분 뒤에는 낡은 답이 된다.
@Slf4j
@Service
@RequiredArgsConstructor
public class QuestSuggestionService {

    // 제안이 살아 있는 시간이다.
    // 이 값을 더하는 기준이 서버 시계가 아니라 이벤트가 들고 온 측정 시각이라는 점이 중요하다.
    // 컨슈머가 잠시 죽었다 되살아나서 밀린 제안을 처리해도 만료 시각이 밀려나지 않는다.
    private static final Duration SUGGESTION_TTL = Duration.ofMinutes(30);

    private final QuestSuggestionRepository questSuggestionRepository;
    private final OutboxRecorder outboxRecorder;
    private final Clock clock;

    // 제안을 저장하면서 slack 카드용 이벤트를 outbox에 적재
    @Transactional
    public SuggestionOutcome store(QuestSuggestionCommand command) {
        // 같은 제안이 두 번 배달되는 것은 카프카에서 정상이다.
        // 오프셋을 커밋하기 전에 죽으면 반드시 다시 오고, Health 쪽 아웃박스가 재발행해도 다시 온다.

        // 이 한 줄이 없으면 두 번째 저장이 기본키 제약에 걸려 예외가 되고,
        // 그 예외는 컨슈머 밖으로 나가 무한 재시도 핸들러를 만난다.
        // 몇 번을 다시 해도 같은 식별자가 같은 제약에 걸리므로 그 파티션이 영원히 멈춘다.
        // 데이터베이스 제약은 두 번째 방어선이고, 조용히 먼저 실행되는 이 검사가 첫 번째다.
        if (questSuggestionRepository.existsBySuggestionId(command.suggestionId())) {
            return discarded(SuggestionOutcome.DUPLICATE_SUGGESTION, command);
        }

        // 컨슈머가 30분 넘게 밀린 뒤 처음 받은 제안이다. 저장하면 수락할 수 없는 카드가 나간다.
        // 만료 시각과 같으면 만료로 본다. 수락 시점의 QuestSuggestion.isExpired 와 같은 규칙이다.
        // 다시 배달돼도 여전히 만료라 또 버려지므로 재배달 중복은 따로 신경 쓸 필요가 없다.
        Instant expiresAt = command.basedOnMeasuredAt().plus(SUGGESTION_TTL);
        if (!clock.instant().isBefore(expiresAt)) {
            return discarded(SuggestionOutcome.EXPIRED_ON_ARRIVAL, command);
        }

        // metric 을 문자열로 받는 이유는 이 값을 만드는 쪽이 AI 라서 세 종류 밖일 수 있기 때문이다.
        // 파싱에 실패했다는 사실 자체가 판정이다.
        Optional<Metric> metric = Metric.from(command.metric());
        if (metric.isEmpty()) {
            return rejected(SuggestionOutcome.UNKNOWN_METRIC, command);
        }
        if (command.targetValue() <= 0) {
            return rejected(SuggestionOutcome.INVALID_TARGET, command);
        }

        QuestSuggestion suggestion = questSuggestionRepository.save(QuestSuggestion.create(
                command.suggestionId(),
                command.userId(),
                command.title(),
                metric.get(),
                command.targetValue(),
                command.activityDate(),
                command.basedOnMeasuredAt(),
                expiresAt
        ));

        // 알림 담당 서비스가 이 이벤트를 받아 Slack 카드를 띄운다.

        // 예전에는 알림 쪽이 Health 의 토픽을 직접 구독하고 내 서비스는 저장만 하는 그림이었다.
        // 그때 근거는 "카드에 필요한 내용이 Health 이벤트에 이미 다 있으니 내가 한 번 거쳐도
        // 정보가 늘지 않는다" 였다. 그 근거가 지금은 성립하지 않는다.

        // 제안의 수명과 상태를 내가 소유하게 됐기 때문이다.
        // 카드의 수락 버튼에 박힐 식별자는 내가 발급한 것이어야 하고,
        // 무엇보다 저장에 실패한 제안은 카드가 나가면 안 된다.
        // 알림이 먼저 나가면 유저가 수락을 눌렀을 때 나는 그런 제안을 모른다고 답하게 되고,
        // 유저는 이유를 알 수 없이 버튼만 안 먹는다.
        // 트렝이드오프는 존재한다. 이제 Slack 발송이 내 서비스의 가용성에 묶인다.
        // 아웃박스에 적재만 하고 발행은 하지 않는다. 발행은 폴링하는 쪽이 따로 한다.
        // 데이터베이스와 카프카를 한 트랜잭션으로 묶을 수 없기 때문에 나온 구조다.
        outboxRecorder.append(
                AggregateType.QUEST_SUGGESTION,
                suggestion.getSuggestionId(),
                OutboxEventType.QUEST_SUGGESTED,
                suggestion.getUserId(),
                // 이벤트가 일어난 시각으로 발행 시각이 아니라 판정 기준 시각을 쓴다.
                // 재발행해도 값이 바뀌지 않아야 소비하는 쪽이 이 값으로 중복을 판단해도 안전하다.
                suggestion.getBasedOnMeasuredAt(),
                QuestToNotification.from(suggestion)
        );

        log.info("quest suggestion stored: suggestionId={} userId={} metric={} target={} expiresAt={}",
                suggestion.getSuggestionId(), suggestion.getUserId(),
                suggestion.getMetric(), suggestion.getTargetVal(), suggestion.getExpiresAt());

        return SuggestionOutcome.STORED;
    }

    // 정상 동작인데 아무 일도 일어나지 않은 경우다. 재배달이 여기에 해당한다.
    private SuggestionOutcome discarded(SuggestionOutcome outcome, QuestSuggestionCommand command) {
        log.info("quest suggestion discarded: outcome={} suggestionId={} userId={}",
                outcome, command.suggestionId(), command.userId());
        return outcome;
    }

    // 보내는 쪽이 계약을 어긴 경우다. 사람이 이유를 알아야함.
    private SuggestionOutcome rejected(SuggestionOutcome outcome, QuestSuggestionCommand command) {
        log.error("quest suggestion rejected: outcome={} suggestionId={} userId={} metric={} target={}",
                outcome, command.suggestionId(), command.userId(),
                command.metric(), command.targetValue());
        return outcome;
    }
}
