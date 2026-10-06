package com.workoutdone.rpgym.game.config;

import com.workoutdone.rpgym.game.quest.adapter.in.kafka.ContractViolationException;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.time.Duration;

// 사실 이 프로젝트는 공부를 하면 할수록 느낀건 굳이 카프카를 쓸 근거가 부족하긴하다.(rabbitMQ로 대체가 더 맞을수도 있다)
// userID로 유저별 순서 보장은 사실상 정합성의 근거는 되지않는다.
// 10:30분 데이터와 10:00데이터가 어떤 오류때문에 거꾸로 도착해도 어짜피 워터마크로 인해 STALE로 무시된다.
// 그리고 리펙토링이 진행되기전 결과로 냈던 자료및 코드는 보관 + 오프셋 되감기로 재처리가 없었다.
// 장애 후 재소비 + DLQ -> 재발행을 현재 진행할 예정이다.(2026-10-02).
// 즉 현재 지금 이 주석을 쓰는 순간에는 메시지 단위 실패 처리가 없기때문에
// 오프셋을 못넘김으로써 파티션 전체가 멈추는 현상이 일어난다.(이걸 HOL Blocking이라고 한다)
// 또한 카프카를 쓰는 이유가 트래픽이 많고 처치량이 많은 환경에서만 적합한데 현재 프로젝트에서 근거로서는
// 매우 부족하다. 내가 생각하기엔 이건 오버엔지니어링이다. 하지만 학습을 위해서 써봣다고 경험치적 관점으로
// 쓴다고 생각하고 실험을 어떻게 하냐에 따라 한계를 실험해볼수있지않을까?
// spike Test 역시 병목은 DB에서 일어났다.
// 그럼 오버엔지니어링이란 관점에서 무엇을 얻었나?라고함면
// 여러 서비스가 같은 이벤트를 독립적으로 각자의 리듬으로 소비하고, 장애 후 되감아 재처리할수 있다는 장점은
// 아직 유효하다.(결국 결과적일관성을 얼마나 지키냐가 핵심이긴하다)
//
// (2026-10-03 추가) 위에서 말한 HOL Blocking 을 예외 분류 + DLT 로 바꿨다.
//   계약 위반(ContractViolationException)  -> 재시도 없이 곧바로 DLT
//   일시 장애(DB · Redis 연결 실패 등)      -> 예전처럼 2초 간격 무한 재시도. 장애 동안의 이벤트가 DLT로 쏟아지면 안 된다
//   그 밖의 예외(NPE, 제약 위반 같은 버그) -> 1초 · 2초 · 4초 세 번 재시도 후 DLT
// DLT 발행이 성공한 뒤에만 원본 오프셋을 커밋한다. 발행이 실패하면 같은 오프셋을 다시 처리하므로 조용한 유실은 없다.
@Configuration
public class KafkaConsumerConfig {

    private static final long RETRY_INTERVAL_MS = 2_000L;
    private static final int MAX_RETRIES = 3;
    private static final Duration DLT_RETENTION = Duration.ofDays(14);
    private static final int DLT_PARTITIONS = 3;

    static final String DLT_SUFFIX = ".DLT";
    static final String DLT_COUNTER = "rpgym.kafka.consumer.dlt";

    // 반드시 하나 알아가야할것이 또 존재한다.
    // 지금 코드상 방어 로직은 정합성만을 기준으로 막는다.
    // 예상치 못한 결정적 실패(NPE,내가 생각지도 못한 데이터 조합, 버그로 인한 제약위반)은
    // 어짜피 항상 실패한다. 이걸 DLQ로 내려보내야하는걸 어떻게 걸르는지가 DLQ 설계의 핵심이다.
    @Bean
    public CommonErrorHandler kafkaErrorHandler(KafkaTemplate<String, String> kafkaTemplate,
                                                MeterRegistry meterRegistry) {
        ExponentialBackOffWithMaxRetries otherBackOff = new ExponentialBackOffWithMaxRetries(MAX_RETRIES);
        otherBackOff.setInitialInterval(1_000L);
        otherBackOff.setMultiplier(2.0);
        return errorHandler(kafkaTemplate, meterRegistry,
                new FixedBackOff(RETRY_INTERVAL_MS, FixedBackOff.UNLIMITED_ATTEMPTS), otherBackOff);
    }

    // 원본 토픽 이름 뒤에 .DLT. Health 의 발행 쪽 DLQ(health.events.dlq)와는 다른 토픽이다.
    // 보존 기간은 원인을 고치는 데 걸리는 시간보다 길어야 한다. 브로커 기본값에 맡기지 않고 여기서 정한다.
    @Bean
    public NewTopic healthEventsDeadLetterTopic(@Value("${rpgym.kafka.health-events-topic}") String topic) {
        return TopicBuilder.name(topic + DLT_SUFFIX)
                .partitions(DLT_PARTITIONS)
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(DLT_RETENTION.toMillis()))
                .build();
    }

    static DefaultErrorHandler errorHandler(KafkaOperations<?, ?> kafka, MeterRegistry meterRegistry,
                                            BackOff transientBackOff, BackOff otherBackOff) {
        // 파티션은 정하지 않고 키(userId)로 나눈다. DLT 파티션 수가 원본보다 적어도 발행이 실패하지 않는다.
        DeadLetterPublishingRecoverer deadLetter = new DeadLetterPublishingRecoverer(kafka,
                (record, ex) -> new TopicPartition(record.topic() + DLT_SUFFIX, -1));

        // 처음부터 0으로 등록해 둔다. 첫 적재 때 생기면 Prometheus increase()가 그 1건을 놓친다.
        Counter dltCounter = Counter.builder(DLT_COUNTER)
                .description("컨슈머가 처리하지 못해 DLT로 옮긴 레코드 수")
                .register(meterRegistry);

        DefaultErrorHandler handler = new DefaultErrorHandler((record, ex) -> {
            // DLT 발행이 실패하면 accept 가 예외를 던진다. 그때는 세지 않고, 원본 오프셋도 커밋되지 않는다.
            deadLetter.accept(record, ex);
            dltCounter.increment();
        }, otherBackOff);
        handler.setBackOffFunction((record, ex) -> isTransient(ex) ? transientBackOff : null);
        handler.addNotRetryableExceptions(ContractViolationException.class);
        return handler;
    }

    // 기다리면 풀리는 장애인 지판 별하는 코드
    // 리스너 예외는 ListenerExecutionFailedException 으로 감싸져 오므로 원인을 끝까지 따라간다.
    // DataAccessResourceFailureException 은 스프링 분류상 "일시적이지 않은" 예외지만 DB · Redis 연결 실패 감지.
    static boolean isTransient(Throwable ex) {
        for (Throwable t = ex; t != null; t = t.getCause()) {
            if (t instanceof TransientDataAccessException
                    || t instanceof RecoverableDataAccessException
                    || t instanceof DataAccessResourceFailureException
                    || t instanceof CannotCreateTransactionException
                    || t instanceof SQLTransientException
                    || t instanceof SQLRecoverableException) {
                return true;
            }
            // 드라이버가 일반 SQLException 으로 던지는 경우. 08 연결 실패, 57P 서버 종료 · 재시작, 53 자원 부족
            if (t instanceof SQLException sql && sql.getSQLState() != null) {
                String state = sql.getSQLState();
                if (state.startsWith("08") || state.startsWith("57P") || state.startsWith("53")) {
                    return true;
                }
            }
            if (t.getCause() == t) {
                break;
            }
        }
        return false;
    }
}
