package com.workoutdone.rpgym.game.quest.application;

import com.workoutdone.rpgym.game.outbox.application.OutboxRecorder;
import com.workoutdone.rpgym.game.outbox.domain.AggregateType;
import com.workoutdone.rpgym.game.outbox.domain.OutboxEventType;
import com.workoutdone.rpgym.game.quest.application.payload.PartyQuestCompletedData;
import com.workoutdone.rpgym.game.quest.domain.QuestStatus;
import com.workoutdone.rpgym.game.quest.domain.aggregate.PartyQuest;
import com.workoutdone.rpgym.game.quest.domain.aggregate.PartyQuestMember;
import com.workoutdone.rpgym.game.quest.domain.repo.PartyQuestMemberRepository;
import com.workoutdone.rpgym.game.quest.domain.repo.PartyQuestRepository;
import com.workoutdone.rpgym.game.quest.domain.vo.ContributionResult;
import com.workoutdone.rpgym.game.quest.domain.vo.Snapshot;
import com.workoutdone.rpgym.game.xp.application.XpGrantService;
import com.workoutdone.rpgym.game.xp.domain.SourceType;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

// 건강 데이터 스냅샷 하나를 그 유저가 참여 중인 파티 퀘스트에 반영한다.

// 이 서비스가 이 파트에서 유일하게 진짜 경합이 생기는 곳이다.
// 개인 퀘스트는 카프카 파티션 키가 userId 라서 한 유저의 이벤트를 한 컨슈머가 순서대로 처리한다.
// 파티는 멤버 네 명이 서로 다른 유저라 서로 다른 파티션에서 오고, 네 스레드가
// 실제로 동시에 같은 파티 퀘스트 행을 건드린다.

// 잘못 짜면 두 가지가 터진다.
// 중복 지급: 합계가 목표에 닿는 순간 네 명이 전부 "완료다" 라고 판단해서
// XP 가 네 배로 나간다

// 완료 누락: 각자 자기 기여만 반영한 뒤 합계를 읽으면, 아직 커밋되지 않은 남의 기여가
// 보이지 않아서 아무도 목표 도달한 내용을 볼수없다.
// 하루짜리 퀘스트라 그 이벤트가 막차면 그대로 미완으로 끝난다
// (중요)두 번째 때문에 멤버 기여분을 그때그때 합산하는 방식을 쓸 수 없고 공유 카운터가 필요하다.
// 첫 번째는 완료를 조건부 UPDATE 로 선점해서 막는다.
// 아래 0~6번에 따라서 파티퀘스트 수행 가드가 진행된다. 데이터의 흐름이 반드시 지켜져야함.
@Slf4j
@Service
@RequiredArgsConstructor
public class PartyQuestProgressService {

    private final PartyQuestRepository partyQuestRepository;
    private final PartyQuestMemberRepository partyQuestMemberRepository;
    private final XpGrantService xpGrantService;
    private final OutboxRecorder outboxRecorder;

    // 실험용
    @Value("${rpgym.experiment.party-tx-delay-ms:0}")
    private long experimentDelayMs;

    // (A-2 반대 실험 전용) normal = 지금 코드 / naive-claim = 완료 여부를 DB 조건부 UPDATE 결과가 아니라
    // 트랜잭션 시작 때 읽은 값으로 자바에서 판단한다. 기본 normal. exp/r12-race 브랜치에만 둔다
    @Value("${rpgym.experiment.party-mode:normal}")
    private String experimentMode;
    // 실험용

    @Transactional
    public Optional<ContributionResult> apply(UUID userId, Snapshot snapshot) {
        // 0. 이 유저가 속한 살아 있는 파티 퀘스트를 찾는다.
        // 만료 판정이 여기 들어 있다. 기준 시각으로 서버 시계가 아니라 이벤트의 측정 시각을 쓴다.
        // 같은 이벤트를 언제 처리하든 결과가 같아야 하기 때문이다.
        // 만료된 퀘스트는 여기서 통째로 걸러지므로 아래 어느 것도 실행되지 않는다.
        // 멤버 행만 갱신되고 카운터는 막히는 식으로 반쪽만 반영되는 경우가 없어서,
        // 카운터와 기여분 합계가 같아야 한다는 등식이 그대로 성립한다.
        Optional<PartyQuest> active = partyQuestRepository.findActiveByUserId(userId, snapshot.measuredAt());
        if (active.isEmpty()) {
            return Optional.empty();
        }
        PartyQuest partyQuest = active.get();
        UUID partyQuestId = partyQuest.getPartyQuestId();

        // 1. 내 멤버 행을 읽는다. 내 행이라 다른 트랜잭션과 겹치지 않는다.
        Optional<PartyQuestMember> found =
                partyQuestMemberRepository.findByPartyQuestIdAndUserId(partyQuestId, userId);
        if (found.isEmpty()) {
            // 0 번 조회가 이 유저의 멤버 행이 있다는 조건으로 찾아온 것이므로 여기 올 수 없다.
            // 온다면 명단이 중간에 지워진 것이라 모니터링에서 걸러야한다.
            log.error("파티 퀘스트는 찾았는데 멤버 행이 없다. partyQuestId={} userId={}", partyQuestId, userId);
            return Optional.empty();
        }
        PartyQuestMember member = found.get();

        // 2. 기여분을 계산하고 멤버 행에 반영한다. 판정은 도메인이 한다.
        ContributionResult result = member.apply(snapshot, partyQuest.getMetric(), partyQuest.activityDate());
        if (result instanceof ContributionResult.Ignored ignored) {
            log.debug("파티 기여 무시. partyQuestId={} userId={} reason={}",
                    partyQuestId, userId, ignored.reason());
            return Optional.of(result);
        }
        partyQuestMemberRepository.save(member);

        int counterDelta = ((ContributionResult.Applied) result).counterDelta();

        // 3. 더할 것이 없으면 공유 카운터를 건드리지 않는다.
        // 기준값이 방금 확정됐거나 누적값이 그대로인 경우다.
        // 건너뛰어도 완료를 놓치지 않는다. 합계가 목표에 닿는 것은 누군가 값을 올렸을 때뿐이고,
        // 값을 올린 트랜잭션은 같은 트랜잭션 안에서 반드시 5 번을 실행하기 때문이다.
        // 경합 지점에 불필요한 잠금을 걸지 않는 것이 여기서 얻는 것이다.
        if (counterDelta == 0) {
            return Optional.of(result);
        }

        // 실험 코드 : 서로 다른 스레드의 파티 트랜잭션 지연을 늘리는 실험
        if (experimentDelayMs > 0) {
            try {
                Thread.sleep(experimentDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        // 실험 코드


        // 4. 공유 카운터를 올린다. 여기가 경합 지점이다.
        // 상태 조건을 걸지 않는다. 걸면 완료 직후 도착한 이벤트에서
        // 멤버 행만 올라가고 합계는 막혀서 둘이 어긋난다.
        partyQuestRepository.addToCurrentValue(partyQuestId, counterDelta);

        // 5. 완료를 선점한다. 동시에 들어온 트랜잭션 중 하나만 1 을 돌려받는다.
        // 완료 여부는 전적으로 이 코드에서 걸러진다.
        int claimed = partyQuestRepository.claimCompletion(partyQuestId, snapshot.measuredAt());

        if ("naive-claim".equals(experimentMode)) {
            // (A-2) 보호 장치를 뺀 판단: 0 번에서 읽어 둔 값(트랜잭션 시작 시점)으로 "내가 완료시켰다"를 정한다.
            // 다른 스레드의 커밋을 못 본 채 판단하므로, 둘이 동시에 완료라고 보거나 아무도 완료라고 못 볼 수 있다.
            boolean javaSaysComplete = partyQuest.getStatus() == QuestStatus.ACTIVE
                    && partyQuest.getCurrentVal() + counterDelta >= partyQuest.getTargetVal();
            // 로그는 영문만: 콘솔 코드페이지를 거치면 한글이 깨져 grep 이 안 된다
            log.warn("[A-2] judge partyQuestId={} read={} delta={} target={} javaComplete={} dbClaimed={}",
                    partyQuestId, partyQuest.getCurrentVal(), counterDelta, partyQuest.getTargetVal(),
                    javaSaysComplete, claimed);
            if (!javaSaysComplete) {
                return Optional.of(result);
            }
            reward(partyQuestId, userId, snapshot.measuredAt());
            return Optional.of(result);
        }
        if (claimed == 0) {
            return Optional.of(result);
        }

        // 6. 선점에 성공한 트랜잭션만 여기 들어온다.
        reward(partyQuestId, userId, snapshot.measuredAt());
        return Optional.of(result);
    }

    // 보상 지급과 이벤트 적재는 위 4, 5 와 같은 트랜잭션이다.
    // 따로 떼면 선점만 성공하고 죽었을 때 상태는 완료인데 보상이 없는 상태가 남고,
    // 상태가 이미 완료라 다시 판정되지도 않는다.
    private void reward(UUID partyQuestId, UUID triggeredBy, Instant measuredAt) {
        // 앞의 두 UPDATE 가 영속성 컨텍스트를 비웠으므로 여기서 읽는 것은 데이터베이스의 현재 값이다.
        // 비우지 않았다면 카운터가 올라가기 전의 옛 값이 그대로 돌아온다.
        PartyQuest completed = partyQuestRepository.findById(partyQuestId)
                .orElseThrow(() -> new IllegalStateException(
                        "방금 완료시킨 파티 퀘스트를 다시 읽을 수 없다. partyQuestId=" + partyQuestId));

        // 지갑을 갱신하는 순서를 userId 로 고정한다.
        // 한 유저가 두 파티에 속하는 것을 막는 제약이 없어서, 두 파티가 동시에 완료되면
        // 한쪽은 A -> B, 다른 쪽은 B -> A 순서로 지갑 행 락을 잡아 서로를 기다릴 수 있다 (데드락).
        // 모든 트랜잭션이 같은 순서로 락을 잡으면 순환 대기가 생기지 않는다.
        // 이걸 정리하자면 식사하는 철학자 문제와 비슷하다. userID를 오름차순으로 정렬하면서
        // 작은 번호를 가지면서 큰 번호에 도달하는 순환대기가 없는 단조 증가에 해당한다.
        List<PartyQuestMember> members = partyQuestMemberRepository.findByPartyQuestId(partyQuestId).stream()
                .sorted(Comparator.comparing(PartyQuestMember::getUserId))
                .toList();

        // 멤버 수만큼 원장에 행이 생긴다. 중복이 아니라 정상적인 패턴.
        // 중복은 같은 멤버에게 두 행이 들어가는 것이고, 그것은 원장의 유니크 제약이 막는다.
        // 멱등키가 (유저, PARTY_QUEST, 파티퀘스트) 이므로 한 멤버는 한 파티 퀘스트에서 한 번만 받는다.
        // 그 제약에 걸려 예외가 나면 이 트랜잭션 전체가 되돌아가고 상태도 진행 중으로 돌아간다.
        // 다시 처리될 때 5 번이 이미 완료된 것을 보고 0 을 돌려주므로 여기 들어오지 않는다.
        // 그래서 예외를 뱉지않는다.
        for (PartyQuestMember member : members) {
            xpGrantService.grant(
                    member.getUserId(), SourceType.PARTY_QUEST, partyQuestId,
                    completed.getRewardXp(), measuredAt);
        }

        // 이벤트 봉투의 userId 로 마지막 기여를 넣은 유저를 쓴다.
        // 이 값이 발행 파티션을 정하는데, 파티 퀘스트 완료는 다른 이벤트와 순서를 맞출 필요가 없어서
        // 누구를 넣어도 판정에 영향이 없다.
        // 누구에게 알릴지는 본문의 명단이 들고 있다.
        outboxRecorder.append(
                AggregateType.PARTY_QUEST,
                partyQuestId,
                OutboxEventType.PARTY_QUEST_COMPLETED,
                triggeredBy,
                measuredAt,
                PartyQuestCompletedData.from(completed, members, measuredAt)
        );

        log.info("파티 퀘스트 완료. partyQuestId={} 멤버={}명 currentVal={} target={} rewardXp={}",
                partyQuestId, members.size(), completed.getCurrentVal(),
                completed.getTargetVal(), completed.getRewardXp());
    }
}
