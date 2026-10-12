package com.workoutdone.rpgym.game.quest.adapter.in.kafka;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

/**
 * health.events 의 Kafka 진입점. 처리는 HealthEventConsumer 에 맡기고, 끝난 뒤 지연만 잰다(R-16).
 *
 * 지연은 "성공한 처리"에만 기록한다. 실패해서 재시도하는 동안은 기록하지 않고, 결국 성공하면
 * 그때 (그 사이 기다린 시간까지 포함한) 값이 한 번 기록된다. 사용자가 실제로 기다린 시간이 그것이다.
 * 예외는 그대로 위로 던진다 -- 재시도 · DLT 분류는 KafkaConsumerConfig 의 에러 핸들러가 한다.
 */
@Component
@RequiredArgsConstructor
public class HealthEventListener {

    private final HealthEventConsumer healthEventConsumer;
    private final ConsumerLatencyMetrics latencyMetrics;

    @KafkaListener(topics = "${rpgym.kafka.health-events-topic}")
    public void onMessage(ConsumerRecord<String, String> record) {
        Optional<Instant> measuredAt = healthEventConsumer.consume(record.value());
        latencyMetrics.record(record.timestamp(), measuredAt);
    }
}
