package com.workoutdone.rpgym.game.ranking.application;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * (R-20) 랭킹 읽기 모델 반영 실패를 센다.
 *
 * 예전에는 log.warn / log.error 만 남겼다. 그 사용자가 다음 XP 를 받을 때까지 랭킹이 틀린 채로 남는데
 * 아무도 몰랐다. 이 카운터가 늘면 알림이 울리고, 대응은 재구축 1회다.
 *
 * stage 태그
 *   redis    : ZSET 반영 실패 (RankingCommandService 가 삼킨 것)
 *   listener : 그 밖의 실패 (레벨 반영 DB 실패 등, XpGrantedEventListener 가 삼킨 것)
 * 두 값 모두 처음부터 0 으로 등록한다. 첫 증가 때 생기면 Prometheus increase() 가 그 1건을 놓친다.
 */
@Component
public class RankingMetrics {

    static final String UPDATE_FAILED = "rpgym.ranking.update.failed";

    private final Counter redisFailed;
    private final Counter listenerFailed;

    public RankingMetrics(MeterRegistry meterRegistry) {
        this.redisFailed = counter(meterRegistry, "redis");
        this.listenerFailed = counter(meterRegistry, "listener");
    }

    public void redisFailed() {
        redisFailed.increment();
    }

    public void listenerFailed() {
        listenerFailed.increment();
    }

    private static Counter counter(MeterRegistry registry, String stage) {
        return Counter.builder(UPDATE_FAILED)
                .description("랭킹 읽기 모델(ZSET · characters.level) 반영에 실패한 횟수")
                .tag("stage", stage)
                .register(registry);
    }
}
