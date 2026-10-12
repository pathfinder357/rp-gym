package com.workoutdone.rpgym.game.outbox.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.workoutdone.rpgym.game.outbox.domain.AggregateType;
import com.workoutdone.rpgym.game.outbox.domain.OutboxEventType;
import com.workoutdone.rpgym.game.outbox.domain.aggregate.OutboxEvent;
import com.workoutdone.rpgym.game.outbox.domain.repo.OutboxEventRepository;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

// 호출하는 쪽의 트랜잭션에 참여(빈에 등록)
// @Transactional이 없어도 이 Component 클래스 메서드는 서비스 계층 내부에서 호출되면
// 기존에 시작된 트랜잭션 범위 안으로 들어감
@Component
@RequiredArgsConstructor
public class OutboxRecorder {

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final OutboxTraceContext outboxTraceContext;

    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEvent append(
            AggregateType aggregateType,
            UUID aggregateId,
            OutboxEventType eventType,
            UUID userId,
            Instant occurredAt,
            Object data
    ) {
        UUID eventId = UUID.randomUUID();
        EventEnvelope envelope = new EventEnvelope(eventId, eventType.name(), occurredAt, userId, data);

        // 기본적으로 기존 트랜잭션을 그대로 이어받아서 실행됨
        // 따라서 별도 @Transactional이 없어도 T2트랜잭션에 묶여서 저장함
        return outboxEventRepository.save(OutboxEvent.pending(
                UUID.randomUUID(),
                aggregateType,
                aggregateId,
                eventType,
                eventId,
                userId,
                toJson(envelope)
        ).withTraceContext(outboxTraceContext.capture()));   // (R-17) 지금 처리 중인 trace 를 같이 남긴다
    }

    private String toJson(EventEnvelope envelope) {
        try {
            return objectMapper.writeValueAsString(envelope);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("failed to serialize " + envelope.eventType(), e);
        }
    }
}
