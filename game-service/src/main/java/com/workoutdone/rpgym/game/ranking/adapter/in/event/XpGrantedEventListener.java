package com.workoutdone.rpgym.game.ranking.adapter.in.event;

import com.workoutdone.rpgym.game.ranking.application.RankingMetrics;
import com.workoutdone.rpgym.game.ranking.application.RankingService;
import com.workoutdone.rpgym.game.xp.application.XpGranted;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;

/**
 * XP 지급을 랭킹 갱신으로 잇는 인바운드 어댑터.
 *
 * <p>xp 컨텍스트는 랭킹을 모른다. {@link XpGranted} 를 발행할 뿐이고 이쪽이 구독한다.
 * 의존은 ranking → xp 한 방향이다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class XpGrantedEventListener {

    private final RankingService rankingService;
    private final RankingMetrics rankingMetrics;

    /**
     * AFTER_COMMIT 이라 XP 지급 트랜잭션이 커밋된 뒤에만 실행된다.
     *
     * <p>여기에 @Transactional 을 걸지 않는다. 트랜잭션 경계는 RankingCommandService.onXpChanged 의
     * REQUIRES_NEW 가 잡는다 (그쪽에 걸어야 하는 이유는 그 메서드 주석 참고).
     *
     * <p>걸면 안 되는 이유 — 여기에 REQUIRES_NEW 를 걸고 서비스가 REQUIRED 로 합류하면,
     * 서비스에서 예외가 나는 순간 물리 트랜잭션이 rollback-only 로 찍힌다. 아래 catch 가 그 예외를
     * 삼켜도 이 메서드가 끝난 뒤 프록시가 커밋을 시도하다 UnexpectedRollbackException 을 던지고,
     * 그건 catch 밖이라 못 잡는다. 실제로 Redis 가 죽었을 때 그 예외가 파티 테스트에서 터졌다.
     */
    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    public void on(XpGranted event) {
        try {
            rankingService.onXpChanged(event.userId());
        } catch (Exception e) {
            /*
             * 랭킹 갱신 실패가 XP 지급을 되돌리거나 컨슈머를 멈춰선 안 된다.
             * AFTER_COMMIT 리스너의 예외는 커밋 호출자에게 전파되므로 여기서 반드시 삼킨다.
             * (삼키지 않으면 Kafka offset 이 안 잡혀 해당 파티션이 멈춘다)
             *
             * 랭킹은 절대값을 다시 읽어 덮어쓰는 구조라, 다음 XpGranted 가 알아서 바로잡는다.
             * 즉시 복구가 필요하면 RankingRebuildJob 을 실행한다(내부 API).
             * 삼키되 세어 둔다. 로그만으로는 틀어진 걸 아무도 모른다.
             */
            rankingMetrics.listenerFailed();
            log.error("랭킹 갱신 실패. userId={}", event.userId(), e);
        }
    }
}
