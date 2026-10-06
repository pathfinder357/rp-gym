package com.workoutdone.rpgym.game.quest.adapter.in.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.workoutdone.rpgym.game.quest.application.QuestProgressService;
import com.workoutdone.rpgym.game.quest.application.QuestSuggestionCommand;
import com.workoutdone.rpgym.game.quest.application.PartyQuestProgressService;
import com.workoutdone.rpgym.game.quest.application.QuestSuggestionService;
import com.workoutdone.rpgym.game.quest.domain.vo.Snapshot;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class HealthEventConsumerTest {

    private static final UUID USER_ID = UUID.fromString("9f1c8e2a-4b7d-4c3e-8a11-2f6d9c0b7e33");
    private static final UUID SUGGESTION_ID = UUID.fromString("7c2e5a91-6f0b-4c88-b3d2-15ae9047cc61");

    @Mock
    private QuestProgressService questProgressService;

    @Mock
    private QuestSuggestionService questSuggestionService;
    
    @Mock
    private PartyQuestProgressService partyQuestProgressService;

    private HealthEventConsumer consumer;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
        consumer = new HealthEventConsumer(
                objectMapper, questProgressService, partyQuestProgressService, questSuggestionService);
    }

    @Test
    @DisplayName("HEALTH_ACTIVITY_SYNCED — cumulative 3필드가 Snapshot으로 옮겨지고 KST가 Instant로 바뀐다")
    void healthActivitySynced() {
        consumer.consume("""
                {
                  "eventId": "b1f4c8e0-3a52-4d17-9c6e-08f1a7d34b90",
                  "eventType": "HEALTH_ACTIVITY_SYNCED",
                  "occurredAt": "2026-08-28T10:30:05+09:00",
                  "userId": "9f1c8e2a-4b7d-4c3e-8a11-2f6d9c0b7e33",
                  "data": {
                    "activityDate": "2026-08-28",
                    "measuredAt": "2026-08-28T10:30:00+09:00",
                    "cumulative": { "steps": 3100, "activeMinutes": 31, "activeCalories": 155 }
                  }
                }
                """);

        ArgumentCaptor<Snapshot> captor = ArgumentCaptor.forClass(Snapshot.class);
        verify(questProgressService).apply(eq(USER_ID), captor.capture());

        Snapshot snapshot = captor.getValue();
        assertThat(snapshot.activityDate()).isEqualTo(LocalDate.of(2026, 8, 28));
        // +09:00 이 UTC로 정규화된다. 09:00을 빼면 01:30Z
        assertThat(snapshot.measuredAt()).isEqualTo(Instant.parse("2026-08-28T01:30:00Z"));
        assertThat(snapshot.steps()).isEqualTo(3100);
        assertThat(snapshot.activeMinutes()).isEqualTo(31);
        assertThat(snapshot.activeCalories()).isEqualTo(155);

        verifyNoInteractions(questSuggestionService);
    }

    @Test
    @DisplayName("QUEST_SUGGESTED — metric은 String 그대로 넘어가고 userId는 envelope에서 온다")
    void questSuggested() {
        consumer.consume("""
                {
                  "eventId": "7c2e5a91-6f0b-4c88-b3d2-15ae9047cc61",
                  "eventType": "QUEST_SUGGESTED",
                  "occurredAt": "2026-08-28T10:30:10+09:00",
                  "userId": "9f1c8e2a-4b7d-4c3e-8a11-2f6d9c0b7e33",
                  "data": {
                    "suggestionId": "7c2e5a91-6f0b-4c88-b3d2-15ae9047cc61",
                    "activityDate": "2026-08-28",
                    "basedOnMeasuredAt": "2026-08-28T10:30:00+09:00",
                    "title": "20분 산책하기",
                    "metric": "ACTIVE_MINUTES",
                    "targetValue": 20
                  }
                }
                """);

        ArgumentCaptor<QuestSuggestionCommand> captor = ArgumentCaptor.forClass(QuestSuggestionCommand.class);
        verify(questSuggestionService).store(captor.capture());

        QuestSuggestionCommand command = captor.getValue();
        assertThat(command.userId()).isEqualTo(USER_ID);
        assertThat(command.suggestionId()).isEqualTo(SUGGESTION_ID);
        assertThat(command.activityDate()).isEqualTo(LocalDate.of(2026, 8, 28));
        assertThat(command.basedOnMeasuredAt()).isEqualTo(Instant.parse("2026-08-28T01:30:00Z"));
        assertThat(command.title()).isEqualTo("20분 산책하기");
        assertThat(command.metric()).isEqualTo("ACTIVE_MINUTES");
        assertThat(command.targetValue()).isEqualTo(20);

        verifyNoInteractions(questProgressService);
    }

    @Test
    @DisplayName("DAILY_GOAL_COMPLETED — MVP 범위 밖이라 소비만 하고 아무 서비스도 부르지 않는다")
    void dailyGoalCompletedIsIgnored() {
        consumer.consume("""
                {
                  "eventId": "0f8b1c2d-3e4f-4a5b-8c9d-0e1f2a3b4c5d",
                  "eventType": "DAILY_GOAL_COMPLETED",
                  "userId": "9f1c8e2a-4b7d-4c3e-8a11-2f6d9c0b7e33",
                  "data": { "activityDate": "2026-08-28" }
                }
                """);

        verifyNoInteractions(questProgressService, questSuggestionService);
    }

    @Test
    @DisplayName("알 수 없는 eventType — 예외를 던지지 않는다. 던지면 그 파티션이 영원히 막힌다")
    void unknownEventTypeDoesNotThrow() {
        assertThatCode(() -> consumer.consume("""
                {
                  "eventId": "0f8b1c2d-3e4f-4a5b-8c9d-0e1f2a3b4c5d",
                  "eventType": "SOMETHING_ELSE",
                  "userId": "9f1c8e2a-4b7d-4c3e-8a11-2f6d9c0b7e33",
                  "data": {}
                }
                """)).doesNotThrowAnyException();

        verifyNoInteractions(questProgressService, questSuggestionService);
    }

    @Test
    @DisplayName("깨진 JSON — 계약 위반 예외를 던진다. 재시도 없이 DLT로 옮겨져 조용히 사라지지 않는다")
    void malformedJsonThrowsContractViolation() {
        assertThatThrownBy(() -> consumer.consume("{ this is not json"))
                .isInstanceOf(ContractViolationException.class);

        verifyNoInteractions(questProgressService, questSuggestionService);
    }

    @Test
    @DisplayName("eventType 누락 — switch가 NPE를 던지기 전에 계약 위반으로 걸러낸다")
    void missingEventTypeThrowsContractViolation() {
        assertThatThrownBy(() -> consumer.consume("""
                {
                  "eventId": "b1f4c8e0-3a52-4d17-9c6e-08f1a7d34b90",
                  "userId": "9f1c8e2a-4b7d-4c3e-8a11-2f6d9c0b7e33",
                  "data": {}
                }
                """)).isInstanceOf(ContractViolationException.class);

        verifyNoInteractions(questProgressService, questSuggestionService);
    }

    @Test
    @DisplayName("cumulative 누락 — NPE 대신 계약 위반 예외. NPE는 재시도할 가치가 있는 예외로 분류된다")
    void missingCumulativeThrowsContractViolation() {
        assertThatThrownBy(() -> consumer.consume("""
                {
                  "eventId": "b1f4c8e0-3a52-4d17-9c6e-08f1a7d34b90",
                  "eventType": "HEALTH_ACTIVITY_SYNCED",
                  "userId": "9f1c8e2a-4b7d-4c3e-8a11-2f6d9c0b7e33",
                  "data": { "activityDate": "2026-08-28", "measuredAt": "2026-08-28T10:30:00+09:00" }
                }
                """)).isInstanceOf(ContractViolationException.class);

        verifyNoInteractions(questProgressService, questSuggestionService);
    }

    @Test
    @DisplayName("QUEST_SUGGESTED data 누락 — 계약 위반 예외를 던진다")
    void missingSuggestionDataThrowsContractViolation() {
        assertThatThrownBy(() -> consumer.consume("""
                {
                  "eventId": "7c2e5a91-6f0b-4c88-b3d2-15ae9047cc61",
                  "eventType": "QUEST_SUGGESTED",
                  "userId": "9f1c8e2a-4b7d-4c3e-8a11-2f6d9c0b7e33"
                }
                """)).isInstanceOf(ContractViolationException.class);

        verifyNoInteractions(questProgressService, questSuggestionService);
    }
}
