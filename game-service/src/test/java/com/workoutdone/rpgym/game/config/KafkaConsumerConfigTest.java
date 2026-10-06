package com.workoutdone.rpgym.game.config;

import com.workoutdone.rpgym.game.quest.adapter.in.kafka.ContractViolationException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.transaction.CannotCreateTransactionException;

import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;

import static org.assertj.core.api.Assertions.assertThat;

// 어떤 예외를 무한 재시도(일시 장애)로 볼지의 분류만 본다.
// 리스너 예외는 ListenerExecutionFailedException 으로 감싸져 오므로 실제와 같은 모양으로 감싸서 넣는다.
class KafkaConsumerConfigTest {

    private static ListenerExecutionFailedException wrapped(Throwable cause) {
        return new ListenerExecutionFailedException("listener failed", cause);
    }

    @Test
    @DisplayName("DB가 내려가 커넥션을 못 얻으면 일시 장애다 — DLT로 쏟아지지 않고 계속 재시도한다")
    void DB_커넥션_실패는_일시_장애다() {
        // Hikari 가 커넥션을 못 주면 JpaTransactionManager 가 이 모양으로 던진다.
        Throwable ex = wrapped(new CannotCreateTransactionException("Could not open JPA EntityManager",
                new SQLTransientConnectionException("HikariPool-1 - Connection is not available")));

        assertThat(KafkaConsumerConfig.isTransient(ex)).isTrue();
    }

    @Test
    @DisplayName("Redis 연결 실패도 일시 장애다 — 스프링 분류상 비일시 예외의 하위지만 기다리면 풀린다")
    void Redis_연결_실패는_일시_장애다() {
        Throwable ex = wrapped(new RedisConnectionFailureException("Unable to connect to Redis"));

        assertThat(KafkaConsumerConfig.isTransient(ex)).isTrue();
    }

    @Test
    @DisplayName("트랜잭션 도중 연결이 끊겨 드라이버가 SQLState 08로 던지면 일시 장애다")
    void SQLState_08은_일시_장애다() {
        Throwable ex = wrapped(new JpaSystemException(new RuntimeException("commit failed",
                new SQLException("An I/O error occurred while sending to the backend.", "08006"))));

        assertThat(KafkaConsumerConfig.isTransient(ex)).isTrue();
    }

    @Test
    @DisplayName("낙관적 락 충돌은 다시 하면 되는 일이라 일시 장애로 본다")
    void 낙관적_락_충돌은_일시_장애다() {
        assertThat(KafkaConsumerConfig.isTransient(wrapped(new OptimisticLockingFailureException("version"))))
                .isTrue();
    }

    @Test
    @DisplayName("버그로 인한 제약 위반 · NPE · 계약 위반은 일시 장애가 아니다 — 몇 번 뒤 DLT로 간다")
    void 결정적_예외는_일시_장애가_아니다() {
        assertThat(KafkaConsumerConfig.isTransient(wrapped(
                new DataIntegrityViolationException("duplicate key", new SQLException("dup", "23505"))))).isFalse();
        assertThat(KafkaConsumerConfig.isTransient(wrapped(new NullPointerException()))).isFalse();
        assertThat(KafkaConsumerConfig.isTransient(wrapped(new ContractViolationException("broken")))).isFalse();
    }
}
