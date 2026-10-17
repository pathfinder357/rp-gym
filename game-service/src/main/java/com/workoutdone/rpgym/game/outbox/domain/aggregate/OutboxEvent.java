package com.workoutdone.rpgym.game.outbox.domain.aggregate;

import com.workoutdone.rpgym.common.entity.BaseCreatedEntity;
import com.workoutdone.rpgym.game.outbox.domain.AggregateType;
import com.workoutdone.rpgym.game.outbox.domain.OutboxEventType;
import com.workoutdone.rpgym.game.outbox.domain.OutboxStatus;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;
import java.util.UUID;

@Entity
@Getter
@Table(name = "outbox_events", schema = "game_service")
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OutboxEvent extends BaseCreatedEntity {

    @Id
    @Column(name = "outbox_id", nullable = false, updatable = false)
    private UUID outboxId;

    @Enumerated(EnumType.STRING)
    @Column(name = "aggregate_type", nullable = false, length = 30, updatable = false)
    private AggregateType aggregateType;

    @Column(name = "aggregate_id", nullable = false, updatable = false)
    private UUID aggregateId;

    @Enumerated(EnumType.STRING)
    @Column(name = "event_type", nullable = false, length = 50, updatable = false)
    private OutboxEventType eventType;

    @Column(name = "event_id", nullable = false, updatable = false, unique = true)
    private UUID eventId;

    @Column(name = "partition_key", nullable = false, length = 50, updatable = false)
    private String partitionKey;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "payload", nullable = false, updatable = false)
    private String payload;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 20)
    private OutboxStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "trace_context", length = 512, updatable = false)
    private String traceContext;

    public static OutboxEvent pending(
            UUID outboxId,
            AggregateType aggregateType,
            UUID aggregateId,
            OutboxEventType eventType,
            UUID eventId,
            UUID userId,
            String payload
    ) {
        OutboxEvent event = new OutboxEvent();
        event.outboxId = outboxId;
        event.aggregateType = aggregateType;
        event.aggregateId = aggregateId;
        event.eventType = eventType;
        event.eventId = eventId;
        event.partitionKey = userId.toString();
        event.payload = payload;
        event.status = OutboxStatus.PENDING;
        event.retryCount = 0;
        return event;
    }

    /** 적재하는 트랜잭션 안에서 한 번만 붙인다(INSERT 전). 이후 바뀌지 않는다 */
    public OutboxEvent withTraceContext(String traceContext) {
        this.traceContext = traceContext;
        return this;
    }

    public void markPublished(Instant publishedAt) {
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = publishedAt;
    }

    /**
     * 발행에 실패했다. 시도 횟수만 올리고 상태는 PENDING으로 남긴다.
     *
     * FAILED로 종료하지 않는 이유는 되살릴 경로가 없어지기 때문이다. 발행 실패의 현실적 원인은
     * 브로커 장애 하나이고 그것은 재시도로 낫는다. PENDING으로 남겨두면 브로커가 살아나는 순간
     * 다음 폴링이 자동으로 집어 간다 -- 유실이 원리적으로 발생하지 않는다.
     */
    public void markRetried() {
        this.retryCount++;
    }
}
