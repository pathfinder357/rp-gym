package com.workoutdone.rpgym.game.ranking.application;

import com.workoutdone.rpgym.game.character.domain.LevelPolicy;
import com.workoutdone.rpgym.game.character.domain.XpClient;
import com.workoutdone.rpgym.game.ranking.domain.CharacterLevelWriter;
import com.workoutdone.rpgym.game.ranking.domain.RankingScore;
import com.workoutdone.rpgym.game.ranking.domain.RankingStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class RankingCommandService implements RankingService{

    private final XpClient xpClient;
    private final CharacterLevelWriter characterLevelWriter;
    private final RankingStore rankingStore;
    private final RankingMetrics rankingMetrics;

    // 실험용 (A-4): 지갑을 읽은 뒤(가) Redis 에 쓰기(다) 전에 넣는 지연. 읽기와 쓰기 사이 틈을 넓힌다
    @Value("${rpgym.experiment.ranking-delay-ms:0}")
    private long experimentRankingDelayMs;
    // true 면 0 ~ ranking-delay-ms 사이 임의 지연(GC 정지 · 스케줄링 · 네트워크 지연 흉내).
    // 2026-10-11 A-4 첫 실행: 모든 리스너에 같은 지연을 넣으면 먼저 읽은 쪽이 먼저 써서 역전이 0 이었다.
    // 역전은 "먼저 읽은 쪽이 읽기와 쓰기 사이에서 더 오래 멈출 때" 만 생긴다
    @Value("${rpgym.experiment.ranking-delay-random:false}")
    private boolean experimentRankingDelayRandom;
    // (A-4 세 번째 설계, 2026-10-11) 지갑을 이 값보다 작게 읽은 리스너만 ranking-stall-ms 만큼 멈춘다.
    // = "옛 값을 읽은 쪽이 하필 그 순간 GC 등으로 멈춘" 최악의 경우를 인위적으로 만든다. 0 이면 끔
    @Value("${rpgym.experiment.ranking-stall-below-xp:0}")
    private int experimentRankingStallBelowXp;
    @Value("${rpgym.experiment.ranking-stall-ms:0}")
    private long experimentRankingStallMs;
    // 실험용

    @Override
    // REQUIRES_NEW 다. AFTER_COMMIT 리스너가 부르는데, 그 시점엔 이미 커밋된 바깥 트랜잭션 리소스가
    // 아직 바인딩돼 있다. 기본 전파(REQUIRED)면 거기 합류해 upsertLevel() 이 조용히 유실된다.
    //
    // 경계가 리스너가 아니라 여기인 이유 — 리스너에 걸면 이 메서드의 예외가 물리 트랜잭션을
    // rollback-only 로 만들고, 리스너의 catch 가 삼켜도 프록시 커밋에서 UnexpectedRollbackException 이 난다.

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void onXpChanged(UUID userId){
        if (userId == null){
            throw new IllegalArgumentException("userId는 필수입니다.");
        }

        // 찾기 쉽게 가,나,다로 명명함.
        // 가~다 사이에 아무런 락이 존재하지 않음으로써 스레드가 커밋될때 이미 락이 풀려있음
        // 그리고 redis는 DB 트랜잭션 밖에 있기도 함.
        // 또 스레드가 커밋된 시점에 지갑을 읽으므로 스레드가 gc나 스케줄링으로 잠깐 멈출 경우에
        // 대한 방어 로직을 위해 기존 코드를 변경함.
        int totalXp = xpClient.findTotalXp(userId); // 지갑을 읽는 시점 (가)
        int level = LevelPolicy.levelOf(totalXp);

        // 실험 코드 (A-4) — 지연은 (가) 지갑 읽기와 (나) 레벨 upsert 사이에 넣는다.
        // 2026-10-11 A-4 세 번째 실행: (나) 뒤에 멈추면 역전 0. (나)의 INSERT ... ON CONFLICT 가 characters 행 락을 잡고
        // 이 REQUIRES_NEW 트랜잭션이 (다) ZADD 뒤에 커밋될 때까지 쥐고 있어서, 같은 유저의 다른 리스너는 (나)에서 줄을 선다.
        // 그래서 (나)~(다) 사이의 정지는 쓰기 순서를 바꾸지 못한다. 역전이 생길 수 있는 틈은 (가)~(나) 뿐이다.
        boolean stall = experimentRankingStallBelowXp > 0 && totalXp < experimentRankingStallBelowXp;
        if (experimentRankingDelayMs > 0 || experimentRankingStallBelowXp > 0) {
            try {
                long sleepMs = experimentRankingDelayRandom
                        ? java.util.concurrent.ThreadLocalRandom.current().nextLong(experimentRankingDelayMs + 1)
                        : experimentRankingDelayMs;
                Thread.sleep(stall ? sleepMs + experimentRankingStallMs : sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // 실험 코드

        characterLevelWriter.upsertLevel(userId, level); // DB에 쓰는 시점 (나)
        // 실험 코드 (A-4)
        if (experimentRankingDelayMs > 0 || experimentRankingStallBelowXp > 0) {
            // 로그는 영문만: 콘솔 코드페이지를 거치면 한글이 깨져 grep 이 안 된다
            log.warn("[A-4] zadd userId={} readXp={}", userId, totalXp);
        }
        // 실험 코드


        // ZADD 는 트랜잭션 밖의 작업이다. DB 커밋이 롤백되어도 Redis 는 되돌아가지 않는다.
        // 다만 다음 onXpChanged() 호출이 절대값으로 다시 덮어쓰므로 자연히 수렴한다.
        //
        // Redis 장애를 여기서 삼킨다. 안 삼키면 예외가 위로 나가 이 트랜잭션이 롤백되고,
        // 재계산 가능한 것(랭킹) 때문에 이미 확정된 것(characters.level)까지 잃는다.
        // 실제로 Redis 를 내린 채 파티 테스트를 돌렸을 때 이 경로가 터졌다.
        try{
            rankingStore.save(userId, RankingScore.encode(level, totalXp)); // (다)
        }catch(Exception e){
            // 다음 동기화가 올때는 괜찮지만 현재 사용자가 보는 화면은 lost update로 보인다.
            // 로그를 남기는게 아니라 카운터로 세서 알림에 알린다.
            rankingMetrics.redisFailed();
            log.warn("랭킹 점수 반영 실패!! 다음 지급 또는 재구축 때 복구 userId={}", userId, e);
        }

        log.debug("랭킹 갱신 userId={} level={} totalXp={}", userId, level, totalXp);
    }
}
