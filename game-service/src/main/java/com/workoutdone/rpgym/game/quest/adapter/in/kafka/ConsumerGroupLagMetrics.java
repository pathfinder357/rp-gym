package com.workoutdone.rpgym.game.quest.adapter.in.kafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;


 // (R-23) 컨슈머 그룹의 "커밋 기준" lag: 파티션 끝 오프셋 −> 그룹이 커밋한 오프셋.

 // (근거) 2026-10-12 실험: 파티션이 20분 멈췄는데 records_lag_max 는 내내 0.

// 이걸 쓰는 이유: 카프카 컨슈머 그룹의 랙(Lag)을 모니터링하거나 수집하는 기능 같은 경우,
// 운영 환경이나 로컬 테스트 환경에 따라 필요할 때만 켜고,
// 평소에는 코드를 고치지 않고 설정(yml) 값 하나로 껐다 켰다하려고.

 // 왜 이 코드가 짜엿을까?: Kafka 클라이언트가 주는 kafka_consumer_fetch_manager_records_lag_max 는 "가져온(fetch) 위치" 기준이다.
 // 한 레코드가 계속 실패하면 에러 핸들러가 그 레코드로 되감고, 컨슈머는 뒤 레코드까지 한 번에 다시 가져온다.
 // 가져온 위치가 파티션 끝에 닿으므로 lag 이 0 으로 계산된다. 밀린 양이 한 번에 가져오는 양(파티션당 최대 1MB)보다
 // 적으면 멈춘 파티션이 보이지 않는다.
 // 실패한 레코드는 커밋되지 않으므로(ack-mode RECORD) 커밋 기준 lag 은 멈춘 동안 0 보다 크게 남는다.

 // 지표: rpgym.game.consumer.group.lag{topic, partition} (파티션 3개라 라벨이 늘지 않는다)
 //       rpgym.game.consumer.group.lag.failed  조회 실패 수(브로커 장애 등). 실패하면 값은 마지막 값을 유지한다

 // 수집 때마다 브로커에 묻지 않고, 전용 스레드가 interval 마다 갱신한다. 스케줄러 스레드(Outbox 발행 200ms)를 붙잡지 않으려고.
 // 앱이 통째로 죽으면 이 값도 사라진다 -> 그 경우는 이 지표가 아니라 up 지표의 몫이다.

@Slf4j
@Component
// 특정 설정값(rpgym.kafka.group-lag.enabled)에 따라 이 스프링 빈을 생성할지 말지 결정하는 역할
@ConditionalOnProperty(prefix = "rpgym.kafka.group-lag", name = "enabled", havingValue = "true", matchIfMissing = true)
// ConsumerGroupLagMetrics 클래스는 카프카 컨슈머가 메시지 lag를 주기적으로 측정
// 이를 micrometer에 전달.
public class ConsumerGroupLagMetrics {

    static final String LAG = "rpgym.game.consumer.group.lag";
    static final String FAILED = "rpgym.game.consumer.group.lag.failed";

    // 그룹이 커밋한 오프셋과 파티션 끝 오프셋을 읽는다. 테스트에서 브로커 없이 바꿔 끼우려고 분리했다
    // 즉 유연하게 카프카 브로커를 늘리는 실험을 하기위해서 설계도를 작성하고
    // 아래 구현체를 통해서 규격만 맞으면 그대로 쓸수있게 하기 위해서이다.
    interface OffsetReader {
        Map<TopicPartition, Long> committed() throws Exception;

        Map<TopicPartition, Long> endOffsets(Iterable<TopicPartition> partitions) throws Exception;
    }

    private final MeterRegistry meterRegistry;
    private final Counter failed;
    private final Map<TopicPartition, AtomicLong> lags = new ConcurrentHashMap<>();
    private final long intervalMs;

    private KafkaAdmin kafkaAdmin;
    private String groupId;
    private OffsetReader reader;
    private AdminClient adminClient;
    private ScheduledExecutorService executor;

    // MeterRegistry: 지표(Metrics)를 수집해서 모니터링 툴(Prometheus 등)로 보내주는 도구
    // KafkaAdmin: 카프카 관리 기능을 수행하는 도구
    // 클래스가 호출될때마다 이 2개를 찾아서 주입
    @Autowired
    public ConsumerGroupLagMetrics(MeterRegistry meterRegistry, KafkaAdmin kafkaAdmin,
                                   @Value("${spring.kafka.consumer.group-id}") String groupId,
                                   @Value("${rpgym.kafka.group-lag.interval-ms:30000}") long intervalMs) {
        this(meterRegistry, null, intervalMs);
        this.kafkaAdmin = kafkaAdmin;
        this.groupId = groupId;
    }

    ConsumerGroupLagMetrics(MeterRegistry meterRegistry, OffsetReader reader, long intervalMs) {
        this.meterRegistry = meterRegistry;
        this.reader = reader;
        this.intervalMs = intervalMs;
        this.failed = Counter.builder(FAILED).description("커밋 기준 lag 조회 실패 수").register(meterRegistry);
    }
    // 스프링이 실행될때 백그라은드 스레드(consumer-group-lag)를 하나 실행
    // 의존성 주입이 끝난 이후에 바로 실행.(사용대기 상태 바로 이전)
    @PostConstruct
    void start() {
        if (reader == null) {
            adminClient = AdminClient.create(kafkaAdmin.getConfigurationProperties());
            reader = new AdminOffsetReader(adminClient, groupId);
        }
        // 백그라운드 전담 일꾼 배정
        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "consumer-group-lag");
            t.setDaemon(true);
            return t;
        });
        executor.scheduleWithFixedDelay(this::refresh, 10_000, intervalMs, TimeUnit.MILLISECONDS);
    }
    // 객체가 메모리에 삭제되기 전에 뒷정리
    @PreDestroy
    void stop() {
        if (executor != null) {
            executor.shutdownNow();
        }
        if (adminClient != null) {
            adminClient.close(Duration.ofSeconds(2));
        }
    }

    void refresh() {
        try {
            Map<TopicPartition, Long> committed = reader.committed();
            if (committed.isEmpty()) {
                return;
            }
            Map<TopicPartition, Long> ends = reader.endOffsets(committed.keySet());
            committed.forEach((tp, offset) -> {
                Long end = ends.get(tp);
                if (end != null) {
                    gauge(tp).set(Math.max(0, end - offset));
                }
            });
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            failed.increment();
            log.warn("컨슈머 그룹 lag 조회 실패. 마지막 값을 유지한다. group={}", groupId, e);
        }
    }

    long current(TopicPartition tp) {
        AtomicLong v = lags.get(tp);
        return v == null ? -1 : v.get();
    }

    private AtomicLong gauge(TopicPartition tp) {
        return lags.computeIfAbsent(tp, k -> {
            AtomicLong value = new AtomicLong();
            Gauge.builder(LAG, value, AtomicLong::get)
                    .description("커밋 기준 컨슈머 lag (파티션 끝 - 커밋 오프셋)")
                    .tag("topic", k.topic())
                    .tag("partition", String.valueOf(k.partition()))
                    .register(meterRegistry);
            return value;
        });
    }

    // 실제 브로커에 묻는 구현
    static final class AdminOffsetReader implements OffsetReader {
        private static final long TIMEOUT_SECONDS = 5;
        private final AdminClient admin;
        private final String groupId;

        AdminOffsetReader(AdminClient admin, String groupId) {
            this.admin = admin;
            this.groupId = groupId;
        }

        @Override
        public Map<TopicPartition, Long> committed() throws Exception {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Map<TopicPartition, Long> result = new HashMap<>();
            offsets.forEach((tp, om) -> {
                if (om != null) {
                    result.put(tp, om.offset());
                }
            });
            return result;
        }

        @Override
        public Map<TopicPartition, Long> endOffsets(Iterable<TopicPartition> partitions) throws Exception {
            Map<TopicPartition, OffsetSpec> request = new HashMap<>();
            partitions.forEach(tp -> request.put(tp, OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> infos =
                    admin.listOffsets(request).all().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Map<TopicPartition, Long> result = new HashMap<>();
            infos.forEach((tp, info) -> result.put(tp, info.offset()));
            return result;
        }
    }
}
