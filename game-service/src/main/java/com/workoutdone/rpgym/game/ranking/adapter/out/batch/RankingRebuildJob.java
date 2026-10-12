package com.workoutdone.rpgym.game.ranking.adapter.out.batch;

import com.workoutdone.rpgym.game.character.adapter.out.persistence.CharacterRepository;
import com.workoutdone.rpgym.game.character.adapter.out.persistence.CharacterRepository.RankingSourceRow;
import com.workoutdone.rpgym.game.character.domain.LevelPolicy;
import com.workoutdone.rpgym.game.ranking.domain.RankingScore;
import com.workoutdone.rpgym.game.ranking.domain.RankingStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Redis 유실 · 반영 실패 뒤 ZSET 을 DB(지갑) 기준으로 다시 채운다.
 *
 * 정기 배치가 아니다. 다음 경우에 내부 API(RankingAdminController)로 한 번 실행한다.
 *   - Redis 를 잃었다
 *   - rpgym.ranking.update.failed 알림이 울렸다
 *   - 정합성 대조(R-15)의 ranking 검사가 0 이 아니다
 *
 * (R-20) 바꾼 점
 *   예전: 사용자마다 XpClient 호출(N번 조회), 점수의 레벨은 characters.level 에서 읽었다.
 *   지금: SQL 한 번에 1,000명씩(키셋 페이지네이션). 레벨은 XP 에서 계산한다(LevelPolicy) --
 *         onXpChanged 와 같은 계산이어야 두 경로의 점수가 같다. characters.level 은 반영 실패로
 *         뒤처져 있을 수 있어 원천으로 쓰지 않는다.
 *   쓰기는 ZADD GT 라, 재구축이 읽는 동안 들어온 새 지급(더 큰 값)을 옛 값으로 덮지 않는다.
 *
 * 트랜잭션을 걸지 않는다. 페이지마다 짧은 조회로 끝내야 긴 스냅샷이 DB 를 붙잡지 않는다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RankingRebuildJob {

    // Redis 는 단일 스레드라 한 번에 너무 많이 보내면 그동안 다른 명령이 전부 막힌다. 끊어서 보낸다.
    static final int BATCH_SIZE = 1000;
    // PostgreSQL 은 UUID 를 바이트 순서(부호 없음)로 비교한다. 가장 작은 값은 0 으로 채운 UUID 다.
    // new UUID(Long.MIN_VALUE, ...) 는 자바 기준 최솟값일 뿐 PostgreSQL 에서는 중간값(0x80...)이라 앞 절반을 건너뛴다.
    private static final UUID MIN_UUID = new UUID(0L, 0L);

    private final CharacterRepository characterRepository;
    private final RankingStore rankingStore;

    // 실험용 (A-10, exp/r12-race 브랜치에만 둔다). 생성자를 바꾸지 않으려고 필드 주입
    @Autowired(required = false)
    private LegacyRankingRebuild legacyRankingRebuild;
    // 실험용

    public long rebuild() {
        return rebuildKeyset(BATCH_SIZE, false);
    }

    // unionQuery: 실험 전용(A-10). true 면 R-22 이전 쿼리로 읽는다
    private long rebuildKeyset(int batchSize, boolean unionQuery) {
        long total = 0;
        UUID after = MIN_UUID;

        while (true) {
            List<RankingSourceRow> rows = unionQuery
                    ? characterRepository.findRankingSourceUnion(after, batchSize)
                    : characterRepository.findRankingSource(after, batchSize);
            if (rows.isEmpty()) {
                break;
            }

            Map<UUID, Double> batch = new LinkedHashMap<>();
            for (RankingSourceRow row : rows) {
                int totalXp = row.getTotalXp() == null ? 0 : row.getTotalXp();
                batch.put(row.getUserId(), RankingScore.encode(LevelPolicy.levelOf(totalXp), totalXp));
            }
            rankingStore.saveAll(batch);

            total += batch.size();
            after = rows.get(rows.size() - 1).getUserId();
            if (rows.size() < batchSize) {
                break;
            }
        }

        log.info("랭킹 재구축 완료. 대상 사용자 수={}", total);
        return total;
    }

    // ---------------------------------------------------------------- 실험 코드 (A-10)
    /**
     * 재구축 방식 · 배치 크기를 바꿔 가며 재구축 시간과, 재구축하는 동안의 랭킹 조회(ZSCORE) 지연을 잰다.
     * 조회 탐침: 별도 스레드가 쉬지 않고 ZSCORE 를 한 번씩 보내고 걸린 시간을 모은다. 재구축 전 3초는 기준선.
     * Redis 는 단일 스레드라 큰 ZADD 를 처리하는 동안 다른 명령이 기다린다. 그 대기가 p99 · 최대값에 드러난다.
     */
    public Map<String, Object> rebuildExperiment(String mode, int batchSize, UUID probeUserId) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("mode", mode);
        result.put("batchSize", batchSize);

        if (probeUserId != null) {
            result.put("probeIdle", summarize(probe(probeUserId, 3000, null)));
        }

        java.util.concurrent.atomic.AtomicBoolean running = new java.util.concurrent.atomic.AtomicBoolean(true);
        List<Long> during = java.util.Collections.synchronizedList(new ArrayList<>());
        Thread prober = null;
        if (probeUserId != null) {
            prober = new Thread(() -> during.addAll(probe(probeUserId, Long.MAX_VALUE, running)), "a10-probe");
            prober.start();
        }

        long t0 = System.nanoTime();
        long rebuilt = "legacy".equals(mode) ? legacyRankingRebuild.rebuild(batchSize)
                : rebuildKeyset(batchSize, "keyset-union".equals(mode));
        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;

        running.set(false);
        if (prober != null) {
            try {
                prober.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            result.put("probeDuring", summarize(during));
        }
        result.put("rebuilt", rebuilt);
        result.put("elapsedMs", elapsedMs);
        log.warn("[A-10] result {}", result);
        return result;
    }

    private List<Long> probe(UUID userId, long maxMs, java.util.concurrent.atomic.AtomicBoolean running) {
        List<Long> micros = new ArrayList<>();
        long end = System.currentTimeMillis() + Math.min(maxMs, 3_600_000L);
        while (System.currentTimeMillis() < end && (running == null || running.get())) {
            long s = System.nanoTime();
            rankingStore.findScore(userId);
            micros.add((System.nanoTime() - s) / 1_000);
        }
        return micros;
    }

    private static Map<String, Object> summarize(List<Long> micros) {
        Map<String, Object> m = new LinkedHashMap<>();
        long[] a;
        synchronized (micros) {
            a = micros.stream().mapToLong(Long::longValue).toArray();
        }
        Arrays.sort(a);
        m.put("count", a.length);
        if (a.length == 0) {
            return m;
        }
        m.put("p50us", a[(int) Math.min(a.length - 1, Math.floor(a.length * 0.50))]);
        m.put("p95us", a[(int) Math.min(a.length - 1, Math.floor(a.length * 0.95))]);
        m.put("p99us", a[(int) Math.min(a.length - 1, Math.floor(a.length * 0.99))]);
        m.put("p999us", a[(int) Math.min(a.length - 1, Math.floor(a.length * 0.999))]);
        m.put("maxUs", a[a.length - 1]);
        return m;
    }
    // ---------------------------------------------------------------- 실험 코드
}
