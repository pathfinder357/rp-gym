package com.workoutdone.rpgym.game.outbox.adapter.out.kafka;

import com.workoutdone.rpgym.game.outbox.application.EventPublisherPort;
import com.workoutdone.rpgym.game.outbox.config.OutboxPublishProperties;
import com.workoutdone.rpgym.game.outbox.domain.OutboxEventType;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
// T1 트랜잭션 끝난후 outbox에 저장된 데이터를 를 외부로 polling
@Component
@RequiredArgsConstructor
public class OutboxKafkaPublisher implements EventPublisherPort {

    /** 소비 측이 payload 역직렬화 전에 타입으로 필터링할 수 있게 하는 헤더. Health가 주는 것과 대칭이다. */
    private static final String HEADER_EVENT_TYPE = "eventType";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxPublishProperties properties;

    // 실험용 (B-12 Outbox 최장 대기 알림, exp/r12-race 브랜치에만 둔다): 이 파일이 있는 동안 발행을 실패시킨다
    // = 브로커에 닿지 못하는 상황. 행은 PENDING 으로 남고 다음 폴링이 다시 집는다. 파일을 지우면 다음 폴링에서 발행된다
    @Value("${rpgym.experiment.publish-fail-flag:}")
    private String experimentPublishFailFlag;

    // 실험용 (B-19 발행측 '최소 한 번', exp/r12-race 브랜치에만 둔다): 이 파일이 있는 동안 발행 성공 N건째 직후
    // JVM 을 그 자리에서 멈춘다(Runtime.halt = kill -9 와 같다. 종료 훅 · 트랜잭션 커밋 없음).
    // Kafka 에는 나갔는데 PUBLISHED 표시는 커밋되지 않은 상태를 만든다
    @Value("${rpgym.experiment.crash-after-send-flag:}")
    private String experimentCrashFlag;
    @Value("${rpgym.experiment.crash-after-sends:10}")
    private int experimentCrashAfterSends;
    private final java.util.concurrent.atomic.AtomicInteger experimentSent = new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public void publish(String topic, String partitionKey, String payload, OutboxEventType eventType) {
        // 실험 코드 (B-12)
        if (experimentPublishFailFlag != null && !experimentPublishFailFlag.isEmpty()
                && java.nio.file.Files.exists(java.nio.file.Path.of(experimentPublishFailFlag))) {
            throw new IllegalStateException("[B-12] injected publish failure topic=" + topic);
        }
        // 실험 코드
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, partitionKey, payload);
        record.headers().add(HEADER_EVENT_TYPE, eventType.name().getBytes(StandardCharsets.UTF_8));

        try {
            kafkaTemplate.send(record).get(properties.sendTimeout().toMillis(), TimeUnit.MILLISECONDS);
            log.debug("kafka 발행 완료. topic={} key={} eventType={}", topic, partitionKey, eventType);
            // 실험 코드 (B-19)
            if (experimentCrashFlag != null && !experimentCrashFlag.isEmpty()
                    && java.nio.file.Files.exists(java.nio.file.Path.of(experimentCrashFlag))) {
                int n = experimentSent.incrementAndGet();
                if (n >= experimentCrashAfterSends) {
                    log.error("[B-19] crash right after send #{} key={} (PUBLISHED 표시 커밋 전)", n, partitionKey);
                    Runtime.getRuntime().halt(137);
                }
                log.info("[B-19] sent #{} key={}", n, partitionKey);
            }
            // 실험 코드

        } catch (InterruptedException e) {
            // 인터럽트 상태를 삼키면 상위에서 종료 신호를 놓친다
            Thread.currentThread().interrupt();
            throw new IllegalStateException("kafka 발행이 중단됐다. topic=" + topic, e);

        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("kafka 발행에 실패했다. topic=" + topic, e);
        }
    }
}
