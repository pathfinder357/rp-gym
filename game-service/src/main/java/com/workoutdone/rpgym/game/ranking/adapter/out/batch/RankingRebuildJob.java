package com.workoutdone.rpgym.game.ranking.adapter.out.batch;

import com.workoutdone.rpgym.game.character.adapter.out.persistence.CharacterRepository;
import com.workoutdone.rpgym.game.character.adapter.out.persistence.CharacterRepository.RankingSourceRow;
import com.workoutdone.rpgym.game.character.domain.LevelPolicy;
import com.workoutdone.rpgym.game.ranking.domain.RankingScore;
import com.workoutdone.rpgym.game.ranking.domain.RankingStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

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
 *
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


    public long rebuild() {
        long total = 0;
        UUID after = MIN_UUID;

        while (true) {
            List<RankingSourceRow> rows = characterRepository.findRankingSource(after, BATCH_SIZE);
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
            if (rows.size() < BATCH_SIZE) {
                break;
            }
        }

        log.info("랭킹 재구축 완료. 대상 사용자 수={}", total);
        return total;
    }
}
