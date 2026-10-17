package com.workoutdone.rpgym.game.config;

import com.workoutdone.rpgym.game.quest.adapter.in.kafka.ContractViolationException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.sql.SQLTransientConnectionException;

import static org.assertj.core.api.Assertions.assertThat;

class DeadLetterLogTest {

    @Test
    @DisplayName("위치 · key · eventId 를 한 줄로 만든다. 깨진 JSON 에서도 eventId 를 건진다")
    void describe() {
        ConsumerRecord<String, String> ok = new ConsumerRecord<>("health.events", 1, 42L, "user-1",
                "{\"eventId\":\"ev-1\",\"userId\":\"user-1\"}");
        ConsumerRecord<String, String> broken = new ConsumerRecord<>("health.events", 0, 7L, "user-2",
                "{ \"eventId\" : \"ev-2\", oops");
        ConsumerRecord<String, String> none = new ConsumerRecord<>("health.events", 0, 8L, null, "{ this is not json");

        assertThat(DeadLetterLog.describe(ok)).isEqualTo("health.events-1@42 key=user-1 eventId=ev-1");
        assertThat(DeadLetterLog.describe(broken)).isEqualTo("health.events-0@7 key=user-2 eventId=ev-2");
        assertThat(DeadLetterLog.describe(none)).isEqualTo("health.events-0@8 key=null eventId=-");
    }

    @Test
    @DisplayName("리스너 예외 껍데기를 벗기고 근본 원인을 한 줄로 남긴다")
    void cause() {
        Exception wrapped = new ListenerExecutionFailedException("Listener method threw",
                new IllegalStateException("버그\n둘째 줄"));

        assertThat(DeadLetterLog.cause(wrapped)).isEqualTo("java.lang.IllegalStateException: 버그 둘째 줄");
        assertThat(DeadLetterLog.cause(new IllegalStateException("x".repeat(400)))).hasSize(
                "java.lang.IllegalStateException: ".length() + 300 + 3);
    }

    @Test
    @DisplayName("재시도 로그의 실패 종류는 에러 핸들러의 분류와 같다")
    void failureKind() {
        Exception dbDown = new ListenerExecutionFailedException("x", new CannotCreateTransactionException("db",
                new SQLTransientConnectionException("down")));
        Exception contract = new ListenerExecutionFailedException("x", new ContractViolationException("bad"));
        Exception bug = new ListenerExecutionFailedException("x", new IllegalStateException("bug"));

        assertThat(KafkaConsumerConfig.failureKind(dbDown)).startsWith("일시 장애");
        assertThat(KafkaConsumerConfig.failureKind(contract)).startsWith("계약 위반");
        assertThat(KafkaConsumerConfig.failureKind(bug)).startsWith("그 밖의 예외");
    }
}
