package com.workoutdone.rpgym.game.quest.application;

import com.workoutdone.rpgym.game.outbox.application.OutboxRecorder;
import com.workoutdone.rpgym.game.outbox.domain.AggregateType;
import com.workoutdone.rpgym.game.outbox.domain.OutboxEventType;
import com.workoutdone.rpgym.game.quest.application.payload.QuestToNotification;
import com.workoutdone.rpgym.game.quest.domain.Metric;
import com.workoutdone.rpgym.game.quest.domain.SuggestionStatus;
import com.workoutdone.rpgym.game.quest.domain.aggregate.QuestSuggestion;
import com.workoutdone.rpgym.game.quest.domain.repo.QuestSuggestionRepository;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

// 제안을 받아서 보관하는 경로의 테스트다.
// 스프링 컨텍스트를 띄우지 않는다. 저장소와 아웃박스를 대체해서 판정만 본다.
class QuestSuggestionServiceTest {

    private static final UUID USER_ID = UUID.randomUUID();
    private static final LocalDate DATE = LocalDate.of(2026, 8, 28);
    private static final Instant BASED_ON = Instant.parse("2026-08-28T01:30:00Z");

    private QuestSuggestionRepository suggestionRepository;
    private OutboxRecorder outboxRecorder;
    private QuestSuggestionService service;

    @BeforeEach
    void setUp() {
        suggestionRepository = mock(QuestSuggestionRepository.class);
        outboxRecorder = mock(OutboxRecorder.class);
        // 기본은 측정 1분 뒤에 받은 것으로 둔다. 만료(30분) 안쪽이다.
        service = serviceAt(BASED_ON.plus(Duration.ofMinutes(1)));

        when(suggestionRepository.existsBySuggestionId(any())).thenReturn(false);
        when(suggestionRepository.save(any())).thenAnswer(call -> call.getArgument(0));
    }

    private QuestSuggestionService serviceAt(Instant now) {
        return new QuestSuggestionService(suggestionRepository, outboxRecorder, Clock.fixed(now, ZoneOffset.UTC));
    }

    private QuestSuggestionCommand command(String metric, int targetValue) {
        return new QuestSuggestionCommand(
                USER_ID, UUID.randomUUID(), DATE, BASED_ON, "20분 산책하기", metric, targetValue);
    }

    private QuestSuggestionCommand validCommand() {
        return command("ACTIVE_MINUTES", 20);
    }

    @Test
    @DisplayName("저장된 제안은 대기 상태이고 아직 퀘스트가 붙어 있지 않다")
    void 저장된_제안은_대기_상태다() {
        ArgumentCaptor<QuestSuggestion> saved = ArgumentCaptor.forClass(QuestSuggestion.class);
        QuestSuggestionCommand command = validCommand();

        assertEquals(SuggestionOutcome.STORED, service.store(command));

        verify(suggestionRepository).save(saved.capture());
        QuestSuggestion suggestion = saved.getValue();
        assertEquals(command.suggestionId(), suggestion.getSuggestionId());
        assertEquals(USER_ID, suggestion.getUserId());
        assertEquals(Metric.ACTIVE_MINUTES, suggestion.getMetric());
        assertEquals(20, suggestion.getTargetVal());
        assertEquals(DATE, suggestion.getActivityDate());
        assertEquals(BASED_ON, suggestion.getBasedOnMeasuredAt());
        assertEquals(SuggestionStatus.PENDING, suggestion.getStatus());
        assertNull(suggestion.getQuestId());
        assertNull(suggestion.getDecidedAt());
    }

    @Test
    @DisplayName("만료 시각은 이벤트가 들고 온 측정 시각에 30분을 더한 값이다 — 서버 시계가 아니다")
    void 만료는_측정_시각_기준_30분이다() {
        ArgumentCaptor<QuestSuggestion> saved = ArgumentCaptor.forClass(QuestSuggestion.class);

        service.store(validCommand());

        verify(suggestionRepository).save(saved.capture());
        // 서버 시계를 썼다면 이 테스트를 돌리는 지금 시각 기준이 되어 값이 매번 달라진다.
        // 측정 시각 기준이라서 몇 년 뒤에 돌려도 같은 값이 나온다.
        assertEquals(BASED_ON.plus(Duration.ofMinutes(30)), saved.getValue().getExpiresAt());
    }

    @Test
    @DisplayName("저장되면 알림용 이벤트가 아웃박스에 적재된다")
    void 저장되면_아웃박스에_적재된다() {
        ArgumentCaptor<QuestToNotification> payload = ArgumentCaptor.forClass(QuestToNotification.class);
        QuestSuggestionCommand command = validCommand();

        service.store(command);

        verify(outboxRecorder).append(
                eq(AggregateType.QUEST_SUGGESTION),
                eq(command.suggestionId()),
                eq(OutboxEventType.QUEST_SUGGESTED),
                eq(USER_ID),
                eq(BASED_ON),
                payload.capture());

        // 카드를 그리는 데 필요한 것만 실린다.
        assertEquals(command.suggestionId(), payload.getValue().suggestionId());
        assertEquals("20분 산책하기", payload.getValue().title());
        assertEquals("ACTIVE_MINUTES", payload.getValue().metric());
        assertEquals(20, payload.getValue().targetValue());
    }

    @Test
    @DisplayName("같은 제안이 다시 배달되면 저장하지 않고 조용히 끝낸다")
    void 재배달은_조용히_버린다() {
        when(suggestionRepository.existsBySuggestionId(any())).thenReturn(true);

        assertEquals(SuggestionOutcome.DUPLICATE_SUGGESTION, service.store(validCommand()));

        // 여기서 저장을 시도하면 기본키 제약에 걸려 예외가 되고,
        // 그 예외는 컨슈머에서 무한 재시도가 되어 파티션을 멈춘다.
        verify(suggestionRepository, never()).save(any());
        verifyNoInteractions(outboxRecorder);
    }

    @Test
    @DisplayName("metric이 세 종류 밖이면 저장하지 않는다 — 만들 수 없는 퀘스트를 카드로 띄우면 안 된다")
    void metric이_세_종류_밖이면_버린다() {
        assertEquals(SuggestionOutcome.UNKNOWN_METRIC, service.store(command("SLEEP_HOURS", 20)));

        verify(suggestionRepository, never()).save(any());
        verifyNoInteractions(outboxRecorder);
    }

    @Test
    @DisplayName("목표값이 0 이하면 저장하지 않는다 — 수락하자마자 완료되어 공짜 XP가 나간다")
    void 목표값이_0_이하면_버린다() {
        assertEquals(SuggestionOutcome.INVALID_TARGET, service.store(command("STEPS", 0)));

        verify(suggestionRepository, never()).save(any());
        verifyNoInteractions(outboxRecorder);
    }

    @Test
    @DisplayName("측정 31분 뒤에 처음 받은 제안은 이미 만료라 저장하지 않고 카드도 보내지 않는다")
    void 만료된_뒤_도착한_제안은_버린다() {
        // 컨슈머가 30분 넘게 밀린 경우다. 저장하면 누를 수 없는 Slack 카드가 나간다.
        service = serviceAt(BASED_ON.plus(Duration.ofMinutes(31)));

        assertEquals(SuggestionOutcome.EXPIRED_ON_ARRIVAL, service.store(validCommand()));

        verify(suggestionRepository, never()).save(any());
        verifyNoInteractions(outboxRecorder);
    }

    @Test
    @DisplayName("만료 시각 정각에 받은 제안도 만료다 — 수락 시점 판정과 같은 반열린 구간")
    void 만료_정각에_도착한_제안도_버린다() {
        service = serviceAt(BASED_ON.plus(Duration.ofMinutes(30)));

        assertEquals(SuggestionOutcome.EXPIRED_ON_ARRIVAL, service.store(validCommand()));

        verify(suggestionRepository, never()).save(any());
        verifyNoInteractions(outboxRecorder);
    }

    @Test
    @DisplayName("측정 29분 뒤에 받은 제안은 아직 살아 있어 저장한다")
    void 만료_전에_도착한_제안은_저장한다() {
        service = serviceAt(BASED_ON.plus(Duration.ofMinutes(29)));

        assertEquals(SuggestionOutcome.STORED, service.store(validCommand()));

        verify(suggestionRepository).save(any());
    }

    @Test
    @DisplayName("어떤 실패에서도 예외를 던지지 않는다 — 예외 하나가 그 파티션 전체를 멈춘다")
    void 실패해도_예외를_던지지_않는다() {
        // 이 경로는 카프카 컨슈머가 부른다.
        // 예외가 컨슈머 밖으로 나가면 오프셋이 커밋되지 않고 같은 메시지를 2초마다 무한히 다시 처리한다.
        // 아래 셋은 몇 번을 다시 해도 같은 결과라 그 파티션이 영원히 멈춘다.
        // 같은 파티션에는 다른 유저의 이벤트도 함께 실려 있다.
        when(suggestionRepository.existsBySuggestionId(any())).thenReturn(true);
        assertDoesNotThrow(() -> service.store(validCommand()));

        when(suggestionRepository.existsBySuggestionId(any())).thenReturn(false);
        assertDoesNotThrow(() -> service.store(command("SLEEP_HOURS", 20)));
        assertDoesNotThrow(() -> service.store(command("STEPS", -1)));
    }
}
