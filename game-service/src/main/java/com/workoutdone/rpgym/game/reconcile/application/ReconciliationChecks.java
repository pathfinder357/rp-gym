package com.workoutdone.rpgym.game.reconcile.application;

import com.workoutdone.rpgym.game.character.domain.LevelPolicy;
import com.workoutdone.rpgym.game.ranking.domain.RankingScore;
import com.workoutdone.rpgym.game.ranking.domain.RankingStore;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * 각 검사는 "어긋난 행" 집합을 정의하고, 개수와 앞쪽 몇 개의 식별자(로그용)를 돌려준다.
 * 실험 때 손으로 돌리던 정합성 SQL 을 상시 작업으로 옮긴 것이다.
 *
 *   ledger_wallet   사용자별 원장 합계 = 지갑 XP            (지급과 지갑 갱신은 같은 트랜잭션)
 *   quest_reward    완료된 개인 퀘스트 1건 = QUEST 원장 1건  (양방향: 완료인데 원장 없음 / 원장인데 완료 아님)
 *   party_counter   파티 퀘스트 current_val = 멤버 기여 합   (V7 주석의 등식)
 *   party_reward    완료된 파티 퀘스트의 원장 수 = 멤버 수   (완료 아닌데 원장 있음도 포함)
 *   ranking         지갑 XP 로 계산한 점수 = ZSET 점수       (Redis 는 커밋 뒤 따로 반영 -> 두 번 연속 어긋날 때만 센다)
 *
 * 비용: 지금 규모(수만 명)에서는 GROUP BY 전체 스캔으로 충분하다. 커지면 원장 created_at 기준으로
 * "최근 바뀐 사용자"만 보도록 좁힌다.
 */
@Component
@RequiredArgsConstructor
public class ReconciliationChecks {

    static final int SAMPLE_SIZE = 20;
    private static final int RANKING_BATCH = 1000;

    private final JdbcTemplate jdbcTemplate;
    private final RankingStore rankingStore;

    public record Result(String check, long mismatches, List<String> sample) {
    }

    private static final String LEDGER_WALLET = """
            SELECT COALESCE(l.user_id, w.user_id)::text AS id
              FROM (SELECT user_id, SUM(amount) AS total
                      FROM game_service.xp_ledgers GROUP BY user_id) l
              FULL OUTER JOIN game_service.wallets w ON w.user_id = l.user_id
             WHERE COALESCE(l.total, 0) <> COALESCE(w.xp, 0)
            """;

    private static final String QUEST_REWARD = """
            SELECT q.quest_id::text AS id
              FROM game_service.quests q
             WHERE q.status = 'COMPLETED'
               AND NOT EXISTS (SELECT 1 FROM game_service.xp_ledgers l
                                WHERE l.source_type = 'QUEST' AND l.source_id = q.quest_id
                                  AND l.user_id = q.user_id)
            UNION ALL
            SELECT l.source_id::text AS id
              FROM game_service.xp_ledgers l
             WHERE l.source_type = 'QUEST'
               AND NOT EXISTS (SELECT 1 FROM game_service.quests q
                                WHERE q.quest_id = l.source_id AND q.status = 'COMPLETED')
            """;

    private static final String PARTY_COUNTER = """
            SELECT pq.party_quest_id::text AS id
              FROM game_service.party_quests pq
              LEFT JOIN (SELECT party_quest_id, SUM(contributed_val) AS total
                           FROM game_service.party_quest_members GROUP BY party_quest_id) m
                ON m.party_quest_id = pq.party_quest_id
             WHERE pq.current_val <> COALESCE(m.total, 0)
            """;

    private static final String PARTY_REWARD = """
            SELECT pq.party_quest_id::text AS id
              FROM game_service.party_quests pq
              LEFT JOIN (SELECT party_quest_id, COUNT(*) AS members
                           FROM game_service.party_quest_members GROUP BY party_quest_id) m
                ON m.party_quest_id = pq.party_quest_id
              LEFT JOIN (SELECT source_id, COUNT(*) AS rewards
                           FROM game_service.xp_ledgers WHERE source_type = 'PARTY_QUEST'
                          GROUP BY source_id) r
                ON r.source_id = pq.party_quest_id
             WHERE (pq.status = 'COMPLETED' AND COALESCE(r.rewards, 0) <> COALESCE(m.members, 0))
                OR (pq.status <> 'COMPLETED' AND COALESCE(r.rewards, 0) > 0)
            """;

    public List<Result> runSqlChecks() {
        return List.of(
                sql("ledger_wallet", LEDGER_WALLET),
                sql("quest_reward", QUEST_REWARD),
                sql("party_counter", PARTY_COUNTER),
                sql("party_reward", PARTY_REWARD)
        );
    }

    /**
     * 지갑이 있는 사용자만 본다(랭킹은 XP 지급 때 처음 들어간다). 1,000명씩 ZMSCORE 로 비교한다.
     * ZSET 점수가 없거나, 지갑 XP 로 계산한 점수와 다르면 어긋난 것이다.
     *
     * 여기서는 "지금 어긋난 사용자 전부"를 돌려줄 뿐 확정하지 않는다. 커밋 직후 AFTER_COMMIT 리스너가
     * 아직 ZSET 을 쓰기 전이면 잠깐 어긋나 보이기 때문이다. 확정은 ReconciliationJob 이
     * "두 번 연속으로 어긋난 사용자"만 세는 방식으로 한다(시각 컬럼 비교는 쓰지 않는다 --
     * wallets.updated_at 은 첫 INSERT 는 JVM 시간대, 이후 UPDATE 는 DB 시간대로 찍혀 기준이 섞여 있다).
     */
    public Set<UUID> findRankingMismatches() {
        Set<UUID> mismatched = new LinkedHashSet<>();
        UUID after = new UUID(0L, 0L);

        while (true) {
            List<Map<String, Object>> rows = jdbcTemplate.queryForList("""
                    SELECT user_id, xp
                      FROM game_service.wallets
                     WHERE user_id > ?
                     ORDER BY user_id
                     LIMIT ?
                    """, after, RANKING_BATCH);
            if (rows.isEmpty()) {
                break;
            }
            List<UUID> ids = rows.stream().map(r -> (UUID) r.get("user_id")).toList();
            Map<UUID, Double> actual = rankingStore.findScores(ids);
            for (Map<String, Object> row : rows) {
                UUID userId = (UUID) row.get("user_id");
                int xp = ((Number) row.get("xp")).intValue();
                double expected = RankingScore.encode(LevelPolicy.levelOf(xp), xp);
                Double score = actual.get(userId);
                if (score == null || score != expected) {
                    mismatched.add(userId);
                }
            }
            after = ids.get(ids.size() - 1);
            if (rows.size() < RANKING_BATCH) {
                break;
            }
        }
        return mismatched;
    }

    private Result sql(String check, String badRows) {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM (" + badRows + ") bad", Long.class);
        List<String> sample = count == null || count == 0
                ? List.of()
                : jdbcTemplate.queryForList("SELECT id FROM (" + badRows + ") bad LIMIT " + SAMPLE_SIZE, String.class);
        return new Result(check, count == null ? 0 : count, sample);
    }
}
