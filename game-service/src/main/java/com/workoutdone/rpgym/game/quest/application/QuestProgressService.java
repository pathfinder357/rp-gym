package com.workoutdone.rpgym.game.quest.application;

import com.workoutdone.rpgym.game.outbox.application.OutboxRecorder;
import com.workoutdone.rpgym.game.outbox.domain.AggregateType;
import com.workoutdone.rpgym.game.outbox.domain.OutboxEventType;
import com.workoutdone.rpgym.game.quest.application.payload.QuestCompletedData;
import com.workoutdone.rpgym.game.quest.domain.aggregate.Quest;
import com.workoutdone.rpgym.game.quest.domain.aggregate.UserLatestSnapshot;
import com.workoutdone.rpgym.game.quest.domain.repo.QuestRepository;
import com.workoutdone.rpgym.game.quest.domain.repo.UserLatestSnapshotRepository;
import com.workoutdone.rpgym.game.quest.domain.vo.ApplyResult;
import com.workoutdone.rpgym.game.quest.domain.vo.Snapshot;
import com.workoutdone.rpgym.game.xp.application.XpGrantService;
import com.workoutdone.rpgym.game.xp.domain.SourceType;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class QuestProgressService {

    private final QuestRepository questRepository;
    private final UserLatestSnapshotRepository userLatestSnapshotRepository;
    private final XpGrantService xpGrantService;
    private final OutboxRecorder outboxRecorder;

    // 실험용 (DLT 재처리, exp/r12-race 브랜치에만 둔다): 이 접두사로 시작하는 유저의 이벤트에서
    // 일부러 예외를 던진다 = "코드 버그". 재시도 3번 뒤 DLT 로 간다. 이 설정을 빼고 다시 띄우는 것이 "버그 수정 배포"
    @Value("${rpgym.experiment.fail-user-prefix:}")
    private String experimentFailUserPrefix;

    // 실험용 (낙관적 락 충돌): 이 접두사 유저의 퀘스트를 읽은 직후, 다른 커넥션에서 version 을 1 올려 커밋한다
    // = "다른 인스턴스가 같은 퀘스트를 먼저 고쳤다"(블루/그린 전환 · 리밸런싱 중 재처리). 저장 때 낙관적 락 충돌이 난다.
    // 퀘스트마다 conflict-times 번까지만 일으킨다. "그 밖의 예외" 재시도(3번)보다 많이 일으켜서,
    // 일시 장애로 분류돼 계속 재시도되는지(DLT 로 가지 않는지)를 본다
    @Value("${rpgym.experiment.conflict-user-prefix:}")
    private String experimentConflictUserPrefix;

    // (ioc 3단계) 의존성 중입때 같이 들어옴
    @Value("${rpgym.experiment.conflict-times:0}")
    private int experimentConflictTimes;

    @Autowired(required = false)
    private JdbcTemplate experimentJdbc;
    // 데이터를 안전하게(동시성이슈없이) 저장하기 위한 내부 작업용 메모리 창고
    // 객체가 생성되는 순간(ioc 1단계)
    private final java.util.concurrent.ConcurrentHashMap<UUID, Integer> experimentConflicts = new java.util.concurrent.ConcurrentHashMap<>();

    // 실험용 (컨슈머 지연 알림): 이 접두사 유저의 이벤트는, stuck-flag 파일이 있는 동안 "일시 장애" 예외를 던진다.
    // 일시 장애는 2초 간격 무한 재시도라 그 파티션이 멈추고 뒤 레코드가 쌓인다(lag). 파일을 지우면 다음 재시도에서 풀린다
    @Value("${rpgym.experiment.stuck-user-prefix:}")
    private String experimentStuckUserPrefix;
    @Value("${rpgym.experiment.stuck-flag:}")
    private String experimentStuckFlag;

    // 실험용 (B-13 처리 지연 알림): 이 접두사 유저의 이벤트는 slow-flag 파일이 있는 동안 slow-ms 만큼 늦게 처리한다.
    // 실패가 아니라 "느림" 이라 재시도 없이 하나씩 끝나며, 끝날 때마다 처리 지연(처리 완료 - Kafka timestamp)이 기록된다
    @Value("${rpgym.experiment.slow-user-prefix:}")
    private String experimentSlowUserPrefix;
    @Value("${rpgym.experiment.slow-flag:}")
    private String experimentSlowFlag;
    @Value("${rpgym.experiment.slow-ms:0}")
    private long experimentSlowMs;
    // 실험용

    // inbound(Driving) HEALTH_ACTIVITY_SYNCED 2 (T2)
    // 함께 커밋 되거나 삭제되는 Inbound
    // user_latest_snapshots, quests, xp_ledgers, wallets, outbox_events
    // 4개의 인스턴스가 함께 커밋 되거나 삭제(롤백)된다.
    @Transactional
    public Optional<ApplyResult> apply(UUID userId, Snapshot snapshot) {
        // 실험 코드 (처리 지연 알림)
        if (experimentSlowMs > 0 && experimentSlowUserPrefix != null && !experimentSlowUserPrefix.isEmpty()
                && userId.toString().startsWith(experimentSlowUserPrefix)
                && experimentSlowFlag != null && !experimentSlowFlag.isEmpty()
                && java.nio.file.Files.exists(java.nio.file.Path.of(experimentSlowFlag))) {
            try {
                Thread.sleep(experimentSlowMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            log.warn("[B-13] slowed userId={} ms={}", userId, experimentSlowMs);
        }
        // 실험 코드 (B-12)
        if (experimentStuckUserPrefix != null && !experimentStuckUserPrefix.isEmpty()
                && userId.toString().startsWith(experimentStuckUserPrefix)
                && experimentStuckFlag != null && !experimentStuckFlag.isEmpty()
                && java.nio.file.Files.exists(java.nio.file.Path.of(experimentStuckFlag))) {
            throw new org.springframework.dao.TransientDataAccessResourceException("[B-12] injected transient failure userId=" + userId);
        }
        // 실험 코드 (A-6)
        if (experimentFailUserPrefix != null && !experimentFailUserPrefix.isEmpty()
                && userId.toString().startsWith(experimentFailUserPrefix)) {
            throw new IllegalStateException("[A-6] injected bug userId=" + userId);
        }
        // 실험 코드

        // inbound(Driving) HEALTH_ACTIVITY_SYNCED 3
        // 활성 Quest 조회보다 먼저 저장해서 Quest가 없어도 누적값을 저장한다.
        storeLatest(userId, snapshot);

        // inbound(Driving) HEALTH_ACTIVITY_SYNCED 4
        // 어댑터를 통한 0층 방어선
        // 'ACTIVE','expired_at'을 걸름
        // 완료된 퀘스트 인스턴스는 애초에 메모리에 올라오지도 않음
        // 영속 상태 객체(영속성 컨텍스트)
        // 같은 이벤트를 언제 처리하든 결과가 같아야함(멱등)-> Instant.now()를 안쓴이유
        Optional<Quest> active = questRepository.findActiveByUserId(userId, snapshot.measuredAt());
        if (active.isEmpty()) {
            return Optional.empty();
        }

        Quest quest = active.get();

        // 실험 코드 (A-8)
        if (experimentConflictTimes > 0 && experimentJdbc != null && experimentConflictUserPrefix != null
                && !experimentConflictUserPrefix.isEmpty() && userId.toString().startsWith(experimentConflictUserPrefix)) {
            int attempt = experimentConflicts.merge(quest.getQuestId(), 1, Integer::sum);
            if (attempt <= experimentConflictTimes) {
                UUID questId = quest.getQuestId();
                // 새 스레드 = 이 트랜잭션과 다른 커넥션(자동 커밋). 같은 스레드면 지금 트랜잭션에 합류해 버린다
                Thread other = new Thread(() -> experimentJdbc.update(
                        "UPDATE game_service.quests SET version = version + 1 WHERE quest_id = ?", questId), "a8-other-instance");
                other.start();
                try {
                    other.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                log.warn("[A-8] conflict injected questId={} userId={} attempt={}", questId, userId, attempt);
            }
        }
        // 실험 코드

        // inbound(Driving) HEALTH_ACTIVITY_SYNCED 5
        // 필드가 바뀌면서 커밋 시점에 Hibernate가 변경 감지로 update 쿼리를 날림
        // 워터마크와 누적값은 이미 DB에 적재됨
        ApplyResult result = quest.applySnapshot(snapshot);
        // .save()를 남긴 이유
        // 1. 포트를 통해서만 저장을 진행(핵사고날적 관점)
        // 2. 변경 감지는 영속상태의 객체일때만 작동함
        // 만약 준영속(detached)가 되면 에러없이 저장이 안되는 불상사 일어남
        // 그리고 추가쿼리도 나가지않음
        questRepository.save(quest);

        if (result instanceof ApplyResult.Completed completed) {
            xpGrantService.grant(
                    userId, SourceType.QUEST, quest.getQuestId(), quest.getRewardXp(), snapshot.measuredAt());
            // pgSQL과 Kafka는 서로 다른 시스템
            // 하나의 트랜잭션으로 묶을수는 없음
            // 반드시 둘중 하나가 먼저 일어나야함
            // 카프카는 롤백이 없으므로 DB 커밋을 먼저 진행해야함
            outboxRecorder.append(
                    AggregateType.QUEST,
                    quest.getQuestId(),
                    OutboxEventType.QUEST_COMPLETED,
                    userId,
                    // EnventEnvelope.occuredAt() -> 스냅샷을 잰 시각
                    snapshot.measuredAt(),
                    QuestCompletedData.from(quest, completed.achievedDelta(), snapshot.measuredAt())
            );

            log.info("quest completed: questId={} userId={} achievedDelta={} rewardXp={} measuredAt={}",
                    quest.getQuestId(), userId, completed.achievedDelta(),
                    quest.getRewardXp(), snapshot.measuredAt());
        } else {
            log.debug("snapshot applied: questId={} userId={} measuredAt={} result={}",
                    quest.getQuestId(), userId, snapshot.measuredAt(), result);
        }

        return Optional.of(result);
    }

    private void storeLatest(UUID userId, Snapshot snapshot) {
        Optional<UserLatestSnapshot> stored = userLatestSnapshotRepository.findByUserId(userId);

        if (stored.isEmpty()) {
            userLatestSnapshotRepository.save(UserLatestSnapshot.create(userId, snapshot));
            return;
        }

        UserLatestSnapshot latest = stored.get();
        if (latest.applyIfNewer(snapshot)) {
            userLatestSnapshotRepository.save(latest);
        }
    }
}
