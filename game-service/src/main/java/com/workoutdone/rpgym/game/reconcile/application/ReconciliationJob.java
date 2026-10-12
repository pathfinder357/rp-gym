package com.workoutdone.rpgym.game.reconcile.application;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;


 // 유실 · 중복을 "막는" 장치(멱등 소비 · 유니크 제약 · 조건부 UPDATE · Outbox)는 이미 있다.
 // 이 작업은 실제로 막혔는지 "확인"한다. 코드만 써놓고 실제로 작동하는지를 모르는건 안하느니 만 못하기 때문.
 // 지금 작업을 좀더 풀어쓰자면 유실 , 중복을 주기적으로 대조해서 어긋난 수를 게이지로 내보내고,
 // 0 이 아니면 Grafana 알림이 Slack 으로 보내는 경비원이다.

 // 어긋난 식별자에 관해서는 로그(ERROR)로 남기고
 // 메트릭 라벨에는 넣지 않는 이유는 카디널리티가 폭발하기 때문임.
 // 지표
 //   rpgym.reconcile.mismatch{check=...}        마지막 대조에서 어긋난 수
 //   rpgym.reconcile.last.success.timestamp     SQL 검사 4개를 마지막으로 끝낸 시각(초). DB 대조가 멈추면 이 값이 멈춘다
 //   rpgym.reconcile.failed{stage=sql|ranking}  대조 실패 횟수. 0 으로 미리 등록

 // (2026-10-11) SQL 검사와 랭킹(Redis) 검사를 따로 실패시킨다. 예전에는 한 try 안에 있어서
 // Redis 가 꺼지면 SQL 검사 4개도 갱신되지 않고 last.success 까지 멈췄다.
 // Redis 장애 때 오히려 DB 정합성 감시가 꺼지는 셈이었다.
 // last.success 를 검사별로 쪼개지 않은 이유: SQL 검사 4개는 같은 DB 를 보므로 하나만 실패하는 일이 드물고,
 // Redis 장애는 이미 ranking-update-failed 알림과 stage=ranking 실패 카운터로 드러난다.
 // 기존 reconcile-stale 알림 규칙(태그 없는 last.success)도 그대로 쓸 수 있다.

 // 인스턴스가 둘(블루/그린 전환 중)이면 이 스케줄러는 2개가 된다.
//  하지만 이 경비원은 읽기작업만 수행 하므로 결과는 같고 부하(select 작업)만 두 배니깐
// 다음에 다룰 주제인 outbox 발행기때는 전환할때에 스케줄러를 체킹해야한다.

@Slf4j
@Component
@ConditionalOnProperty(prefix = "rpgym.reconcile", name = "enabled", havingValue = "true", matchIfMissing = true)
public class ReconciliationJob {

    static final String MISMATCH = "rpgym.reconcile.mismatch";
    static final String LAST_SUCCESS = "rpgym.reconcile.last.success.timestamp";
    static final String FAILED = "rpgym.reconcile.failed";
    static final List<String> CHECKS = List.of("ledger_wallet", "quest_reward", "party_counter", "party_reward", "ranking");

    private final ReconciliationChecks checks;
    private final Clock clock;
    // 지난 대조에서 랭킹이 어긋났던 사용자. 이번에도 어긋나야 확정한다.
    private Set<UUID> previousRankingMismatches = Set.of();
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();
    private final AtomicLong lastSuccessEpochSeconds = new AtomicLong();
    private final Counter sqlFailed;
    private final Counter rankingFailed;

    // 실험용 (B-13 대조 멈춤 알림, exp/r12-race 브랜치에만 둔다): 이 파일이 있는 동안 대조를 건너뛴다 = 작업이 멈춘 상황.
    // last.success 가 멈추고 30분이 지나면 game-reconcile-stale 이 울려야 한다
    @Value("${rpgym.experiment.reconcile-pause-flag:}")
    private String experimentPauseFlag;
    // 실험용

    public ReconciliationJob(ReconciliationChecks checks, MeterRegistry meterRegistry, Clock clock) {
        this.checks = checks;
        this.clock = clock;
        // 처음부터 0 으로 등록한다. 첫 대조 전에도 시계열이 있어야 "데이터 없음" 과 "0" 이 구분된다.
        for (String check : CHECKS) {
            AtomicLong value = new AtomicLong();
            gauges.put(check, value);
            Gauge.builder(MISMATCH, value, AtomicLong::get)
                    .description("정합성 대조에서 어긋난 행 수(0 이어야 정상)")
                    .tag("check", check)
                    .register(meterRegistry);
        }
        Gauge.builder(LAST_SUCCESS, lastSuccessEpochSeconds, AtomicLong::get)
                .description("정합성 대조를 마지막으로 끝낸 시각(epoch seconds)")
                .register(meterRegistry);
        this.sqlFailed = failedCounter(meterRegistry, "sql");
        this.rankingFailed = failedCounter(meterRegistry, "ranking");
    }

    private static Counter failedCounter(MeterRegistry meterRegistry, String stage) {
        return Counter.builder(FAILED)
                .description("정합성 대조 실패 횟수")
                .tag("stage", stage)
                .register(meterRegistry);
    }

    // 서버 켜지고 1분뒤에 첫 실행, 그후엔 10분마다 계속 순찰도셈.
    @Scheduled(initialDelayString = "${rpgym.reconcile.initial-delay:60000}",
               fixedDelayString = "${rpgym.reconcile.interval:600000}")
    public void run() {
        // 실험 코드 (B-13)
        if (experimentPauseFlag != null && !experimentPauseFlag.isEmpty()
                && java.nio.file.Files.exists(java.nio.file.Path.of(experimentPauseFlag))) {
            log.warn("[B-13] reconcile paused by flag");
            return;
        }
        // 실험 코드
        // 둘 다 스케줄러 밖으로 새면 다음 실행이 멈춘다. 각자 삼키고 카운터로 남긴다.
        runSqlChecks();
        runRankingCheck();
    }

    private void runSqlChecks() {
        try {
            apply(checks.runSqlChecks());
            lastSuccessEpochSeconds.set(clock.instant().getEpochSecond());
        } catch (Exception e) {
            sqlFailed.increment();
            log.error("정합성 대조 실패. stage=sql", e);
        }
    }

    private void runRankingCheck() {
        try {
            apply(List.of(confirmRanking(checks.findRankingMismatches())));
        } catch (Exception e) {
            rankingFailed.increment();
            // "두 번 연속" 은 연달아 성공한 두 번의 관측이어야 한다. 실패를 사이에 두고 이어 붙이지 않는다.
            // 게이지는 마지막으로 확인한 값 그대로 둔다(모르는 값을 0 으로 덮지 않는다).
            previousRankingMismatches = Set.of();
            log.error("정합성 대조 실패. stage=ranking (SQL 검사는 계속 갱신된다)", e);
        }
    }


     // 랭킹은 커밋 뒤(AFTER_COMMIT) 따로 반영되므로 대조하는 순간 잠깐 어긋나 보일 수 있다.
     // 두 번 연속(기본 10분 간격) 어긋난 사용자만 센다. 반영이 늦은 것이라면 다음 대조 때는 맞아 있다.
    // 그리고 애초에 redis를 보기 때문에 따로 때어내는것이 맞음. 어짜피 pgSQL DB만 바라보게 하고 이건 별개임.

    ReconciliationChecks.Result confirmRanking(Set<UUID> current) {
        Set<UUID> confirmed = new LinkedHashSet<>(current);
        confirmed.retainAll(previousRankingMismatches);
        previousRankingMismatches = Set.copyOf(current);
        List<String> sample = confirmed.stream().limit(ReconciliationChecks.SAMPLE_SIZE).map(UUID::toString).toList();
        return new ReconciliationChecks.Result("ranking", confirmed.size(), sample);
    }

    void apply(List<ReconciliationChecks.Result> results) {
        for (ReconciliationChecks.Result r : results) {
            AtomicLong gauge = gauges.get(r.check());
            if (gauge != null) {
                gauge.set(r.mismatches());
            }
            if (r.mismatches() > 0) {
                log.error("정합성 불일치. check={} count={} sample={}", r.check(), r.mismatches(), r.sample());
            } else {
                log.debug("정합성 대조 통과. check={}", r.check());
            }
        }
    }

    long current(String check) {
        return gauges.get(check).get();
    }
}
