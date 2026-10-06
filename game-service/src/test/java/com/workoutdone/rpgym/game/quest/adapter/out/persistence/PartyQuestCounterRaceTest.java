package com.workoutdone.rpgym.game.quest.adapter.out.persistence;

import com.workoutdone.rpgym.game.config.JpaAuditingConfig;
import com.workoutdone.rpgym.game.party.adapter.out.persistence.PartyJpaRepository;
import com.workoutdone.rpgym.game.party.domain.PartyMetric;
import com.workoutdone.rpgym.game.party.domain.PartyVisibility;
import com.workoutdone.rpgym.game.party.domain.aggregate.Party;
import com.workoutdone.rpgym.game.quest.domain.Metric;
import com.workoutdone.rpgym.game.quest.domain.QuestStatus;
import com.workoutdone.rpgym.game.quest.domain.aggregate.PartyQuest;
import com.workoutdone.rpgym.game.quest.domain.aggregate.PartyQuestMember;
import com.workoutdone.rpgym.game.quest.domain.vo.ContributionResult;
import com.workoutdone.rpgym.game.quest.domain.vo.Snapshot;

import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.shaded.org.checkerframework.checker.units.qual.A;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;


 // 공유 카운터 UPDATE 에 상태 조건을 걸지 않은 이유
 // 상황: 목표 4,000 · 멤버 4명 · 카운터 3,000.
 //  1. A 의 트랜잭션이 퀘스트를 ACTIVE 로 읽고 자기 멤버 행에 +500 을 반영한다.
 //   2. A 가 카운터를 올리기 직전에 D 의 트랜잭션이 +1,000 으로 완료를 선점하고 커밋한다.
 //   3. A 가 카운터를 올린다.
 // 3 에서 카운터에 상태 조건이 있으면 A 의 +500 은 0행으로 버려지고,
 // 멤버 기여분 합계(4,500)와 카운터(4,000)가 어긋난다.
 // 1 과 2 사이의 끼어들기는 래치 대신 "A 트랜잭션 안에서 D 트랜잭션을 다른 스레드로 끝까지 돌리는" 방식으로 고정한다.
 // 두 트랜잭션이 잡는 행이 겹치지 않아(A 는 자기 멤버 행, D 는 자기 멤버 행 + 카운터 행) 기다림 없이 끝난다.


@Testcontainers(disabledWithoutDocker = true)
@DataJpaTest(properties = {
        "spring.jpa.hibernate.ddl-auto=validate",
        "spring.flyway.schemas=game_service"
})
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(JpaAuditingConfig.class)
// 트랜잭션 두 개가 서로의 커밋을 봐야 하므로 테스트 트랜잭션을 끈다. 각 테스트는 자기가 만든 id 로만 단언한다.
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class PartyQuestCounterRaceTest {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void datasource(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
    }

    private static final ZoneId KST = ZoneId.of("Asia/Seoul");
    private static final LocalDate DAY = LocalDate.of(2026, 10, 4);
    private static final int TARGET = 4_000;
    private static final int BASELINE = 1_000;

    @Autowired PartyJpaRepository partyJpa;
    @Autowired PartyQuestJpaRepository partyQuestJpa;
    @Autowired PartyQuestMemberJpaRepository memberJpa;
    @Autowired PlatformTransactionManager txManager;
    @PersistenceContext EntityManager em;

    // 카운터를 올리는 방법만 바꿔 끼운다. 나머지 절차는 PartyQuestProgressService.apply 의 2 · 4 · 5 단계와 같다.
    @FunctionalInterface
    interface CounterUpdate {
        int add(UUID partyQuestId, int delta);
    }

    // 지금 코드: 상태 조건 없음
    private CounterUpdate current() {
        return (id, delta) -> partyQuestJpa.addToCurrentValue(id, delta);
    }

    // 트레이드오프 A: 카운터에도 상태 조건을 건다
    private CounterUpdate guarded() {
        return (id, delta) -> em.createNativeQuery(
                        "update game_service.party_quests set current_val = current_val + :delta "
                                + "where party_quest_id = :id and status = 'ACTIVE'")
                .setParameter("delta", delta)
                .setParameter("id", id)
                .executeUpdate();
    }

    record Outcome(int counterRows, int claimed) {}

    @DisplayName("카운터에 상태 조건이 없으면 완료 직후 도착한 기여도 더해져 카운터와 멤버 기여분 합계가 같다")
    @Test
    void 상태_조건이_없으면_합계가_맞는다() throws Exception {
        Race race = runRace(current());

        assertThat(race.d().claimed()).isEqualTo(1);       // 완료는 D 가 가져갔다
        assertThat(race.a().counterRows()).isEqualTo(1);   // A 의 +500 도 카운터에 들어갔다
        assertThat(race.a().claimed()).isEqualTo(0);       // A 는 완료를 다시 가져가지 못한다 (중복 지급 없음)

        PartyQuest after = readQuest(race.questId());
        assertThat(after.getStatus()).isEqualTo(QuestStatus.COMPLETED);
        assertThat(after.getCurrentVal()).isEqualTo(4_500);
        assertThat(sumOfContributions(race.questId())).isEqualTo(after.getCurrentVal());
    }

    @DisplayName("카운터에 상태 조건을 걸면 완료 직후 도착한 기여가 0행으로 버려져 카운터와 멤버 기여분 합계가 어긋난다")
    @Test
    void 상태_조건을_걸면_합계가_어긋난다() throws Exception {
        Race race = runRace(guarded());

        assertThat(race.d().claimed()).isEqualTo(1);
        assertThat(race.a().counterRows()).isEqualTo(0);   // A 의 +500 이 버려졌다

        PartyQuest after = readQuest(race.questId());
        assertThat(after.getCurrentVal()).isEqualTo(4_000);
        assertThat(sumOfContributions(race.questId())).isEqualTo(4_500);
    }
        UUID c = UUID.randomUUID();

        record Race(UUID questId, Outcome a, Outcome d) {}

        private PartyQuestCounterRaceTest.Race runRace(CounterUpdate counter) throws Exception {
            TransactionTemplate tx = new TransactionTemplate(txManager);
            UUID a = UUID.randomUUID();
            UUID b = UUID.randomUUID();
        UUID d = UUID.randomUUID();
        UUID questId = createQuest(tx, a, b, c, d);

        // A · B · C 가 1,000 씩 기여 → 카운터 3,000, 아직 완료 아님
        contribute(tx, counter, questId, a, 2_000, at(10, 0));
        contribute(tx, counter, questId, b, 2_000, at(10, 1));
        contribute(tx, counter, questId, c, 2_000, at(10, 2));

        ExecutorService other = Executors.newSingleThreadExecutor();
        try {
            final Outcome[] dOutcome = new Outcome[1];
            Outcome aOutcome = tx.execute(s -> {
                // 1. A 는 퀘스트를 ACTIVE 로 읽었다. 컨슈머가 이벤트를 처리할 때 가장 먼저 하는 조회다.
                assertThat(partyQuestJpa.findActiveByUserId(a, QuestStatus.ACTIVE, at(10, 30))).isNotEmpty();
                int delta = applyToMember(questId, a, 2_500, at(10, 30)); // A 의 기여 1,000 → 1,500 (+500)

                // 2. A 가 카운터를 올리기 전에 D 가 다른 트랜잭션에서 완료를 선점하고 커밋한다.
                try {
                    dOutcome[0] = other.submit(() -> contribute(tx, counter, questId, d, 2_000, at(10, 3))).get();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }

                // 3. A 가 카운터를 올리고 완료 선점을 시도한다.
                int rows = counter.add(questId, delta);
                int claimed = partyQuestJpa.claimCompletion(questId, at(10, 30), QuestStatus.ACTIVE, QuestStatus.COMPLETED);
                return new Outcome(rows, claimed);
            });
            return new Race(questId, aOutcome, dOutcome[0]);
        } finally {
            other.shutdown();
        }
    }

    // PartyQuestProgressService.apply 의 2 · 4 · 5 단계를 한 트랜잭션으로
    private Outcome contribute(TransactionTemplate tx, CounterUpdate counter,
                               UUID questId, UUID userId, int cumulative, Instant measuredAt) {
        return tx.execute(s -> {
            int delta = applyToMember(questId, userId, cumulative, measuredAt);
            int rows = counter.add(questId, delta);
            int claimed = partyQuestJpa.claimCompletion(questId, measuredAt, QuestStatus.ACTIVE, QuestStatus.COMPLETED);
            return new Outcome(rows, claimed);
        });
    }

    private int applyToMember(UUID questId, UUID userId, int cumulative, Instant measuredAt) {
        PartyQuestMember member = memberJpa.findByPartyQuestIdAndUserId(questId, userId).orElseThrow();
        ContributionResult result = member.apply(new Snapshot(DAY, measuredAt, cumulative, 0, 0), Metric.STEPS, DAY);
        memberJpa.saveAndFlush(member);
        return ((ContributionResult.Applied) result).counterDelta();
    }

    private UUID createQuest(TransactionTemplate tx, UUID... members) {
        return tx.execute(s -> {
            Instant start = DAY.atStartOfDay(KST).toInstant();
            Instant end = DAY.plusDays(1).atStartOfDay(KST).toInstant().minusSeconds(1);
            UUID partyId = partyJpa.saveAndFlush(Party.create(
                    UUID.randomUUID(), "race", members[0], PartyVisibility.PUBLIC, PartyMetric.STEPS, 4, start,
                    Duration.ofHours(24), Duration.ofDays(7))).getId();
            PartyQuest quest = partyQuestJpa.saveAndFlush(PartyQuest.create(
                    UUID.randomUUID(), partyId, "4,000보 함께 걷기", Metric.STEPS, TARGET, 100, start, end));
            for (UUID userId : members) {
                memberJpa.save(PartyQuestMember.join(UUID.randomUUID(), quest.getPartyQuestId(), userId, BASELINE));
            }
            memberJpa.flush();
            return quest.getPartyQuestId();
        });
    }

    private PartyQuest readQuest(UUID questId) {
        return new TransactionTemplate(txManager).execute(s -> partyQuestJpa.findById(questId).orElseThrow());
    }

    private int sumOfContributions(UUID questId) {
        return new TransactionTemplate(txManager).execute(s -> memberJpa.findByPartyQuestId(questId).stream()
                .mapToInt(PartyQuestMember::getContributedVal)
                .sum());
    }

    private static Instant at(int hour, int minute) {
        return DAY.atTime(hour, minute).atZone(KST).toInstant();
    }
}
