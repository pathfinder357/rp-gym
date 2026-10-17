package com.workoutdone.rpgym.game.reconcile.application;

import com.workoutdone.rpgym.game.character.adapter.out.persistence.CharacterRepository;
import com.workoutdone.rpgym.game.character.adapter.out.persistence.CharacterRepository.RankingSourceRow;
import com.workoutdone.rpgym.game.config.JpaAuditingConfig;
import com.workoutdone.rpgym.game.ranking.FakeRankingStore;
import com.workoutdone.rpgym.game.ranking.domain.RankingScore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 일부러 어긋난 데이터를 넣고, 검사마다 정확히 그 행만 잡는지 본다.
 */
@Testcontainers(disabledWithoutDocker = true)
@DataJpaTest(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.flyway.schemas=game_service"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
class ReconciliationChecksIntegrationTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    @Autowired JdbcTemplate jdbc;
    @Autowired CharacterRepository characterRepository;

    private FakeRankingStore rankingStore;
    private ReconciliationChecks checks;

    @BeforeEach
    void setUp() {
        jdbc.execute("TRUNCATE game_service.xp_ledgers, game_service.wallets, game_service.party_quest_members, "
                + "game_service.party_quests, game_service.quest_suggestions, game_service.quests, "
                + "game_service.characters CASCADE");
        rankingStore = new FakeRankingStore();
        checks = new ReconciliationChecks(jdbc, rankingStore);
    }

    @DisplayName("R-15: 어긋난 데이터만 정확히 잡는다 -- 원장≠지갑, 완료 퀘스트 원장 누락, 파티 카운터≠기여 합, 파티 보상 수≠멤버 수")
    @Test
    void sqlChecksCatchOnlyBrokenRows() {
        UUID ok = UUID.randomUUID();
        UUID broken = UUID.randomUUID();
        ledger(ok, "QUEST", UUID.randomUUID(), 100);
        wallet(ok, 100);
        ledger(broken, "QUEST", UUID.randomUUID(), 80);
        wallet(broken, 50);                                     // ledger_wallet 1건 (+ quest_reward: 완료 퀘스트 없는 원장)

        UUID questOk = quest(ok, "COMPLETED");
        ledger(ok, "QUEST", questOk, 10);
        jdbc.update("UPDATE game_service.wallets SET xp = xp + 10 WHERE user_id = ?", ok);
        quest(ok, "COMPLETED");                                 // quest_reward: 완료인데 원장 없음

        UUID party = party();
        UUID pqFine = partyQuest(party, "COMPLETED", 300);
        member(pqFine, ok, 200);
        member(pqFine, broken, 100);
        ledger(ok, "PARTY_QUEST", pqFine, 5);
        ledger(broken, "PARTY_QUEST", pqFine, 5);
        jdbc.update("UPDATE game_service.wallets SET xp = xp + 5 WHERE user_id IN (?, ?)", ok, broken);

        UUID pqBroken = partyQuest(party, "COMPLETED", 999);    // party_counter: 999 ≠ 70
        member(pqBroken, ok, 70);
        member(pqBroken, broken, 0);
        ledger(ok, "PARTY_QUEST", pqBroken, 5);                 // party_reward: 멤버 2명인데 보상 1건
        jdbc.update("UPDATE game_service.wallets SET xp = xp + 5 WHERE user_id = ?", ok);

        Map<String, Long> result = checks.runSqlChecks().stream()
                .collect(Collectors.toMap(ReconciliationChecks.Result::check, ReconciliationChecks.Result::mismatches));

        assertThat(result).containsEntry("ledger_wallet", 1L)
                .containsEntry("quest_reward", 3L)
                .containsEntry("party_counter", 1L)
                .containsEntry("party_reward", 1L);
    }

    @DisplayName("R-15: 정상 데이터에서는 모든 검사가 0")
    @Test
    void cleanDataIsZero() {
        UUID user = UUID.randomUUID();
        UUID q = quest(user, "COMPLETED");
        ledger(user, "QUEST", q, 30);
        wallet(user, 30);

        assertThat(checks.runSqlChecks()).allMatch(r -> r.mismatches() == 0);
    }

    @DisplayName("R-15 · R-20: 랭킹 검사는 지갑 XP 로 계산한 점수와 ZSET 점수가 다르거나 없는 사용자를 찾는다")
    @Test
    void rankingCheck() {
        UUID same = UUID.randomUUID();
        UUID stale = UUID.randomUUID();
        UUID missing = UUID.randomUUID();
        for (UUID u : List.of(same, stale, missing)) {
            wallet(u, 250);
        }
        rankingStore.save(same, RankingScore.encode(3, 250));
        rankingStore.save(stale, RankingScore.encode(2, 150));

        assertThat(checks.findRankingMismatches()).containsExactlyInAnyOrder(stale, missing);
    }

    @DisplayName("R-20: 레벨 upsert 는 GREATEST 라 낮은 레벨이 늦게 와도 내려가지 않는다")
    @Test
    void upsertLevelNeverLowers() {
        UUID user = UUID.randomUUID();
        characterRepository.upsertLevel(UUID.randomUUID(), user, 7);
        characterRepository.upsertLevel(UUID.randomUUID(), user, 5);

        Integer level = jdbc.queryForObject("SELECT level FROM game_service.characters WHERE user_id = ?", Integer.class, user);
        assertThat(level).isEqualTo(7);
    }

    @DisplayName("R-20: 재구축 원천은 characters 와 wallets 의 사용자를 합치고, 지갑이 없으면 0")
    @Test
    void rankingSourceUnionsCharactersAndWallets() {
        UUID onlyCharacter = UUID.randomUUID();
        UUID onlyWallet = UUID.randomUUID();
        characterRepository.upsertLevel(UUID.randomUUID(), onlyCharacter, 1);
        wallet(onlyWallet, 420);

        List<RankingSourceRow> rows = characterRepository.findRankingSource(new UUID(0L, 0L), 10);

        Map<UUID, Integer> byUser = rows.stream().collect(Collectors.toMap(RankingSourceRow::getUserId, RankingSourceRow::getTotalXp));
        assertThat(byUser).containsEntry(onlyCharacter, 0).containsEntry(onlyWallet, 420).hasSize(2);
    }

    @DisplayName("R-22: 작은 페이지로 끝까지 넘겨도 두 테이블 사용자가 정확히 한 번씩, 지갑 값과 함께 나온다")
    @Test
    void rankingSourcePagesEveryUserOnceWithSmallPages() {
        // 세 갈래를 섞는다: 캐릭터만 / 지갑만 / 둘 다. 페이지 크기 3 이면 경계가 여러 번 갈림
        Map<UUID, Integer> expected = new java.util.HashMap<>();
        for (int i = 0; i < 20; i++) {
            UUID user = UUID.randomUUID();
            switch (i % 3) {
                case 0 -> { characterRepository.upsertLevel(UUID.randomUUID(), user, 1); expected.put(user, 0); }
                case 1 -> { wallet(user, 10 * i); expected.put(user, 10 * i); }
                default -> { characterRepository.upsertLevel(UUID.randomUUID(), user, 1); wallet(user, 10 * i); expected.put(user, 10 * i); }
            }
        }

        Map<UUID, Integer> seen = new java.util.LinkedHashMap<>();
        UUID after = new UUID(0L, 0L);
        while (true) {
            List<RankingSourceRow> page = characterRepository.findRankingSource(after, 3);
            if (page.isEmpty()) {
                break;
            }
            for (RankingSourceRow row : page) {
                assertThat(seen.put(row.getUserId(), row.getTotalXp())).as("중복 " + row.getUserId()).isNull();
            }
            after = page.get(page.size() - 1).getUserId();
        }

        assertThat(seen).isEqualTo(expected);
    }

    private void ledger(UUID user, String type, UUID source, int amount) {
        jdbc.update("INSERT INTO game_service.xp_ledgers (ledger_id, user_id, source_type, source_id, amount, occurred_at) "
                + "VALUES (?, ?, ?, ?, ?, now())", UUID.randomUUID(), user, type, source, amount);
    }

    private void wallet(UUID user, int xp) {
        jdbc.update("INSERT INTO game_service.wallets (user_id, xp) VALUES (?, ?)", user, xp);
    }

    private UUID quest(UUID user, String status) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, "
                + "baseline_val, baseline_measured_at, status, reward_xp, expired_at) "
                + "VALUES (?, ?, ?, 't', 'STEPS', 100, 0, now(), ?, 10, now() + interval '1 day')",
                id, user, UUID.randomUUID(), status);
        return id;
    }

    private UUID party() {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO game_service.parties (id, party_name, owner_id, metric, matching_deadline_at, ends_at) "
                + "VALUES (?, 'p', ?, 'STEPS', now(), now() + interval '7 days')", id, UUID.randomUUID());
        return id;
    }

    private UUID partyQuest(UUID party, String status, int current) {
        UUID id = UUID.randomUUID();
        jdbc.update("INSERT INTO game_service.party_quests (party_quest_id, party_id, title, metric, target_val, "
                + "current_val, status, reward_xp, started_at, expired_at) "
                + "VALUES (?, ?, 't', 'STEPS', 100, ?, ?, 5, now(), now() + interval '1 day')", id, party, current, status);
        return id;
    }

    private void member(UUID partyQuest, UUID user, int contributed) {
        jdbc.update("INSERT INTO game_service.party_quest_members (party_quest_member_id, party_quest_id, user_id, "
                + "baseline_val, contributed_val) VALUES (?, ?, ?, 0, ?)", UUID.randomUUID(), partyQuest, user, contributed);
    }
}
