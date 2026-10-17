package com.workoutdone.rpgym.game.outbox.application;

import com.workoutdone.rpgym.game.outbox.config.OutboxPublishProperties;
import com.workoutdone.rpgym.game.outbox.domain.aggregate.OutboxEvent;
import com.workoutdone.rpgym.game.outbox.domain.repo.OutboxEventRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * Outbox -> Kafka 발행기의 한 라운드.
 *
 * 적재(OutboxRecorder)와 발행(여기)이 분리된 것이 Outbox 패턴의 요점이다.
 * DB와 Kafka는 한 트랜잭션으로 묶을 수 없으므로, 이벤트를 같은 DB에 먼저 적재해 원자성을 얻고
 * 발행은 나중에 따로 한다. 발행이 실패하면 PENDING으로 남아 다음 라운드가 다시 집는다.
 *
 * 상태 변경은 더티 체킹으로 반영되므로 save 호출이 없다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxRelay {

    private final OutboxEventRepository outboxEventRepository;
    private final EventPublisherPort eventPublisherPort;
    private final OutboxPublishProperties properties;
    private final OutboxTraceContext outboxTraceContext;

    /** @return 이번 라운드에 발행 성공한 건수 */
    @Transactional
    public int relayOnce() {
        List<OutboxEvent> pending = outboxEventRepository.findPendingForUpdate(properties.pollSize());
        if (pending.isEmpty()) {
            return 0;
        }

        int published = 0;

        for (OutboxEvent event : pending) {
            if (!relay(event)) {
                /*
                 * 한 건이라도 실패하면 라운드를 중단한다.
                 *
                 * 1) 뒤 이벤트를 먼저 보내면 같은 파티션 안에서 순서가 뒤집힌다.
                 * 2) 브로커 장애라면 남은 건도 전부 타임아웃이라 라운드 전체가 sendTimeout x 건수만큼 지연된다.
                 *
                 * 남은 PENDING은 그대로 남아 다음 폴링에서 다시 집힌다.
                 */
                log.warn("발행 실패로 이번 라운드를 중단한다. 성공={} 남은 대상={}",
                        published, pending.size() - published);
                break;
            }
            published++;
        }

        return published;
    }

    private boolean relay(OutboxEvent event) {
        try {
            /*
             * payload 컬럼의 문자열을 그대로 보낸다.
             * 발행 시점에 JSON을 다시 만들면 event_id가 재발행마다 바뀌어
             * 소비 측의 중복 처리 방지가 무력화된다.
             */
            outboxTraceContext.runInContext(event.getTraceContext(), "outbox publish", () ->
                    eventPublisherPort.publish(
                            properties.topic(),
                            event.getPartitionKey(),
                            event.getPayload(),
                            event.getEventType()
                    ));

            event.markPublished(Instant.now());
            log.debug("이벤트 발행 완료. eventType={} eventId={}", event.getEventType(), event.getEventId());
            return true;

        } catch (Exception e) {
            /*
             * Kafka 발행 실패는 DB 트랜잭션을 오염시키지 않으므로, 여기서 잡아도
             * markRetried()의 변경이 정상 커밋된다. (제약 위반 같은 DB 예외라면 반대다)
             */
            event.markRetried();
            // 일정횟수를 넘으면 DLQ 대신에 로그 레벨을 올린다.
            logFailure(event, e);
            return false;
        }
    }

    /**
     * 상태는 PENDING으로 유지하고 로그 레벨만 올린다.
     *
     * 최대 재시도 후 FAILED로 종료하지 않는 이유는 그 상태를 되살릴 경로가 없기 때문이다.
     * "브로커 다운 후 자동 재발행"이 요구사항이므로, 포기하는 순간 그 요구사항을 못 지킨다.
     */
    private void logFailure(OutboxEvent event, Exception e) {
        if (event.getRetryCount() >= properties.escalateAfter()) {
            log.error("발행 실패가 {}회 누적됐다. 브로커 상태를 확인해야 한다. eventId={} eventType={}",
                    event.getRetryCount(), event.getEventId(), event.getEventType(), e);
            return;
        }
        log.warn("발행 실패. 다음 폴링에서 재시도한다. eventId={} 시도={} 원인={}",
                event.getEventId(), event.getRetryCount(), e.toString());
    }
}
