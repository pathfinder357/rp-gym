package com.workoutdone.rpgym.game.outbox.application;

import com.workoutdone.rpgym.game.outbox.domain.repo.OutboxEventRepository;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.LocalDateTime;


//   Outbox 발행이 멈췄는지 보는 지표.
//  개수가 아니라 가장 오래 기다린 이벤트의 시간을 측정함. 개수는 트래픽에 따라서  의존성이 너무 높다. 예를 들어
//  (정각엔 정상인데 수백 건, 새벽엔 장애여도 몇 건). 나이는 트래픽과 무관하게 "안 빠지고 있다"만 보여준다.
//  브로커 장애 · 발행 스케줄러 정지 · 계속 실패하는 이벤트 셋 다 이 값이 커지는 것으로 똑같이 드러난다.
//  발행기 안에서 재지 않는다. 스케줄러가 멈추면 그 값도 멈춰 정상으로 보인다.
//  그래서 Prometheus 가 수집할 때마다 DB 를 직접 읽는다. PENDING 부분 인덱스를 타므로 수집 주기(15초)에 부담이 없다.
//  기준은 적재 시각(created_at)이다. 이벤트의 occurredAt 은 측정 시각이라 늦게 온 이벤트면 처음부터 오래돼 보인다.
//  created_at 은 JPA Auditing 이 JVM 기본 시간대의 LocalDateTime 으로 넣으므로 여기서도 같은 방식으로 지금을 잰다.
//  Health 의 rpgym.outbox.* 지표와 이름이 겹치지 않게 한다. Health 쪽 알림 규칙은 job 으로 거르지 않는다.
@Component
public class OutboxMetrics {

    static final String OLDEST_PENDING_AGE = "rpgym.game.outbox.oldest.pending.age";

    public OutboxMetrics(OutboxEventRepository outboxEventRepository, MeterRegistry meterRegistry) {
        Gauge.builder(OLDEST_PENDING_AGE, outboxEventRepository, OutboxMetrics::oldestPendingAgeSeconds)
                .description("아직 발행되지 않은 Outbox 이벤트 중 가장 오래 기다린 것의 대기 시간")
                .baseUnit("seconds")
                .register(meterRegistry);
    }

    private static double oldestPendingAgeSeconds(OutboxEventRepository repository) {
        return repository.findOldestPendingCreatedAt()
                .map(oldest -> Duration.between(oldest, LocalDateTime.now()).toMillis() / 1000.0)
                .orElse(0.0);
    }
}
