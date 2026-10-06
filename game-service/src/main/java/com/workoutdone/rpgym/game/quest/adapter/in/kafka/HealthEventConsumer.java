package com.workoutdone.rpgym.game.quest.adapter.in.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.workoutdone.rpgym.game.quest.adapter.in.kafka.dto.HealthActivitySyncedData;
import com.workoutdone.rpgym.game.quest.adapter.in.kafka.dto.HealthEventEnvelope;
import com.workoutdone.rpgym.game.quest.adapter.in.kafka.dto.QuestSuggestedData;
import com.workoutdone.rpgym.game.quest.application.PartyQuestProgressService;
import com.workoutdone.rpgym.game.quest.application.QuestProgressService;
import com.workoutdone.rpgym.game.quest.application.QuestSuggestionCommand;
import com.workoutdone.rpgym.game.quest.application.QuestSuggestionService;
import com.workoutdone.rpgym.game.quest.application.SuggestionOutcome;
import com.workoutdone.rpgym.game.quest.domain.vo.ApplyResult;
import com.workoutdone.rpgym.game.quest.domain.vo.ContributionResult;
import com.workoutdone.rpgym.game.quest.domain.vo.Snapshot;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * health.events 토픽 하나에서 3종을 받아 분기한다.
 *
 * 이 클래스가 하는 일은 셋뿐이다 -- 역직렬화 / 도메인 타입으로 변환 / 서비스 호출.
 * 트랜잭션을 열지 않고(경계는 서비스에 있다), 판정을 하지 않는다.
 *
 * 특히 HEALTH_ACTIVITY_SYNCED를 "활성 Quest가 있나"로 미리 거르지 않는다.
 * 그 판단은 QuestProgressService 안에 있고, 여기서 거르면 baseline 조달 설계가 무너진다.
 *
 * 실패를 어떤 예외로 던지느냐가 이 클래스의 핵심 결정이다. 처리 방식은 KafkaConsumerConfig 가 정한다.
 *   계약 위반(깨진 JSON · 필수 필드 누락) -> ContractViolationException -> 재시도 없이 곧바로 DLT
 *   DB 다운 같은 일시 장애                -> 서비스 예외 그대로     -> 무한 재시도 (유실 0)
 *   그 밖의 예외(버그 등)                 -> 서비스 예외 그대로     -> 몇 번 재시도 후 DLT
 * 예전에는 계약 위반을 로그만 남기고 정상 리턴했다. 파티션은 안 막히지만 메시지가 조용히 사라졌다.
 * 알 수 없는 eventType 은 여전히 로그만 남기고 넘긴다. Health 가 새 타입을 추가해도 Game 이 멈추면 안 된다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class HealthEventConsumer {

    private static final String HEALTH_ACTIVITY_SYNCED = "HEALTH_ACTIVITY_SYNCED";
    private static final String QUEST_SUGGESTED = "QUEST_SUGGESTED";
    private static final String DAILY_GOAL_COMPLETED = "DAILY_GOAL_COMPLETED";
    private static final int MAX_TITLE_LENGTH = 100;

    private final ObjectMapper objectMapper;
    private final QuestProgressService questProgressService;
    private final PartyQuestProgressService partyQuestProgressService;
    private final QuestSuggestionService questSuggestionService;

    // inbound(Driving) HEALTH_ACTIVITY_SYNCED, QUEST_SUGGESTED 트랜잭션을 시작하기 위한 어댑터
    // 순서대로 T1, T2로 명명
    // T1은 컨슈머가 apply, T2는 컨슈머가 store
    @KafkaListener(topics = "${rpgym.kafka.health-events-topic}")
    public void consume(String message) {
        HealthEventEnvelope envelope;
        try {
            envelope = objectMapper.readValue(message, HealthEventEnvelope.class);
        } catch (JsonProcessingException e) {
            // 재시도해도 같은 문자열이 같은 곳에서 깨진다. 재시도 없이 DLT로 보낸다.
            throw new ContractViolationException("health event 역직렬화 실패", e);
        }

        // eventType이 null이면 아래 switch가 NPE를 던지고, 그 NPE는 재시도 대상으로 분류되어 헛돈다.
        // 컨슈머가 ACK를 보내지 못하고, 메시지 소비 -> NPE -> NACK(카프카가 실제로 이걸받진않지만 관용적표현임) -> 메시지큐 offset 미전진
        // userId가 null이면 그대로 서비스로 내려가 저장 시점에 터진다.
        if (envelope.eventType() == null || envelope.userId() == null) {
            throw new ContractViolationException("envelope 필수 필드 누락. eventType=%s userId=%s"
                    .formatted(envelope.eventType(), envelope.userId()));
        }

        // eventId는 여기서만 알 수 있다. 아래 서비스들의 로그에도 붙도록 MDC에 넣는다.
        MDC.put("eventId", String.valueOf(envelope.eventId()));
        MDC.put("userId", String.valueOf(envelope.userId()));
        try {
            dispatch(envelope); // 메서드 안에 찍히는 모든 로그에 eventId, userId 붙음
        } finally {
            MDC.remove("eventId"); // 서버나 메시지 컨슈머는 성능때문에 스레드 풀 방식으로 스레드 재사용
            // MDC를 지우지 않으면 다음 다른 메시지 처리할때 이전 메시지의 MDC 정보가 남아서 지워야함.
            MDC.remove("userId");
        }
    }

    private void dispatch(HealthEventEnvelope envelope) {
        switch (envelope.eventType()) {
            case HEALTH_ACTIVITY_SYNCED -> applySnapshot(envelope);
            case QUEST_SUGGESTED -> storeSuggestion(envelope);
            // MVP 범위 밖. 소비는 하되 아무것도 하지 않는다 -- 안 받으면 offset이 안 밀린다.
            case DAILY_GOAL_COMPLETED -> log.debug("DAILY_GOAL_COMPLETED는 MVP 범위 밖이라 무시한다.");
            default -> log.error("알 수 없는 eventType={}", envelope.eventType());
        }
    }
    // T1 입구
    private void applySnapshot(HealthEventEnvelope envelope) {
        HealthActivitySyncedData data = convert(envelope.data(), HealthActivitySyncedData.class);
        if (data.activityDate() == null || data.measuredAt() == null || data.cumulative() == null) {
            throw new ContractViolationException("HEALTH_ACTIVITY_SYNCED 필수 필드 누락. data=" + envelope.data());
        }

        Snapshot snapshot = new Snapshot(
                data.activityDate(),
                data.measuredAt().toInstant(),
                data.cumulative().steps(),
                data.cumulative().activeMinutes(),
                data.cumulative().activeCalories()
        );

        Optional<ApplyResult> result = questProgressService.apply(envelope.userId(), snapshot);
        log.debug("HEALTH_ACTIVITY_SYNCED 개인 퀘스트 처리 완료. measuredAt={} result={}",
                snapshot.measuredAt(), result.map(Object::toString).orElse("NO_ACTIVE_QUEST"));

        // 같은 스냅샷을 파티 퀘스트에도 반영한다.
        // 두 서비스가 각자 트랜잭션을 연다. 하나로 묶지 않는 이유는 둘이 독립이기 때문이다.
        // 파티 쪽에서 문제가 생겼다고 개인 퀘스트 판정과 XP 지급을 되돌릴 이유가 없다.
        // 파티 쪽이 예외를 던지면 컨슈머가 이 이벤트를 다시 처리하는데,
        // 개인 퀘스트는 이미 반영한 시각 이하의 스냅샷을 무시하므로 두 번 반영되지 않는다.
        Optional<ContributionResult> partyResult =
                partyQuestProgressService.apply(envelope.userId(), snapshot);
        log.debug("HEALTH_ACTIVITY_SYNCED 파티 퀘스트 처리 완료. measuredAt={} result={}",
                snapshot.measuredAt(), partyResult.map(Object::toString).orElse("NO_ACTIVE_PARTY_QUEST"));
    }

    // T2 입구
    // 제안을 받아서 보관만 한다. 퀘스트는 여기서 만들지 않는다.
    // 유저가 Slack 카드에서 수락을 눌렀을 때 HTTP 로 들어와서 만들어진다.
    private void storeSuggestion(HealthEventEnvelope envelope) {
        QuestSuggestedData data = convert(envelope.data(), QuestSuggestedData.class);
        // 필수 필드 검사에서 title 추가
        if (data.suggestionId() == null || data.activityDate() == null || data.basedOnMeasuredAt() == null
        || data.title() == null || data.title().isEmpty()) {
            throw new ContractViolationException("QUEST_SUGGESTED 필수 필드 누락. data=" + envelope.data());
        }

        String title = data.title();
        if (title.length() > MAX_TITLE_LENGTH) {
            // Health는 하루에 한번만 제안한다. 하지만 내쪽에서는 제안이 몇번이 오든
            // 활성화 퀘스트는 단 1개이다.
            log.warn("title이 {}자를 넘어 자른다. suggestionId={} length={}", MAX_TITLE_LENGTH,
                data.suggestionId(), title.length());
            title= clampTitle(title);
        }

        SuggestionOutcome outcome = questSuggestionService.store(new QuestSuggestionCommand(
                envelope.userId(),
                data.suggestionId(),
                data.activityDate(),
                data.basedOnMeasuredAt().toInstant(),
                title, // 앞에 지역변수에서 자른 값 대입
                data.metric(),
                data.targetValue()
        ));

        // 폐기 사유별 로그 레벨은 서비스가 정한다. 여기서는 eventId와의 연결만 남긴다.
        log.debug("QUEST_SUGGESTED 처리 완료. suggestionId={} outcome={}", data.suggestionId(), outcome);
    }

    private <T> T convert(JsonNode data, Class<T> type) {
        if (data == null || data.isNull()) {
            throw new ContractViolationException("data가 비어 있다. type=" + type.getSimpleName());
        }
        try {
            return objectMapper.treeToValue(data, type);
        } catch (JsonProcessingException | IllegalArgumentException e) {
            throw new ContractViolationException("data 변환 실패. type=%s data=%s".formatted(type.getSimpleName(), data), e);
        }
    }

    private static String clampTitle(String title) {
        if (title.length() <= MAX_TITLE_LENGTH) {
            return title;
        }
        String cut = title.substring(0, MAX_TITLE_LENGTH);
        // 서로게이트 쌍 중간을 자르면 깨진 문자가 남는다 (AI가 이모지를 붙이는 경우)
        if (Character.isHighSurrogate(cut.charAt(cut.length() - 1))) {
            cut = cut.substring(0, cut.length() - 1);
        }
        return cut;
    }

}
