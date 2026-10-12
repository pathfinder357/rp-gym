package com.workoutdone.rpgym.game.xp.application;

import com.workoutdone.rpgym.game.xp.domain.SourceType;
import com.workoutdone.rpgym.game.xp.domain.aggregate.Wallet;
import com.workoutdone.rpgym.game.xp.domain.aggregate.XpLedger;
import com.workoutdone.rpgym.game.xp.domain.repo.WalletRepository;
import com.workoutdone.rpgym.game.xp.domain.repo.XpLedgerRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

// XP 지급의 유일한 입구. 퀘스트 · 파티 퀘스트 · 업적이 전부 여기를 거친다.
// 중복 지급 방어는 두 겹이다. 두 겹이 막는 상황이 다르다.
//   1겹 조용한 검사  : 정상 경로의 재처리(같은 이벤트 재수신, 업적 재평가). 예외 없이 "이미 지급됨" 으로 끝낸다.
//   2겹 유니크 제약  : 두 트랜잭션이 동시에 1겹을 통과한 경합 (리밸런싱 중 같은 파티션이 겹칠 때).예외가 나고 트랜잭션 전체가 롤백된다.
// 1겹이 없으면 왜 안 되나:
// 유니크 위반 예외는 이 메서드를 부른 쪽의 트랜잭션까지 롤백 전용으로 만든다(REQUIRED 로 참여하므로).
// 호출부가 catch 해도 커밋 시점에 UnexpectedRollbackException 이 난다.
// 그러면 퀘스트 상태 · 이벤트 적재까지 같이 사라지고, 컨슈머는 그 이벤트를 무한 재시도한다.
// "이미 준 것" 은 오류가 아니라 정상 결과이므로 예외가 아니라 값으로 돌려준다.
// 1겹만 있으면 왜 안 되나:
// 조회와 INSERT 사이에 틈이 있다. 두 트랜잭션이 동시에 "없음" 을 보면 둘 다 INSERT 한다.
// 그 틈은 DB 가 원자적으로 판단하는 제약만 닫을 수 있다.
// 경합으로 2겹이 발동하면 트랜잭션 전체가 롤백되고 컨슈머가 다시 처리하는데,
// 그때는 먼저 커밋된 행이 보이므로 1겹에서 조용히 끝난다.
@Slf4j
@Service
@RequiredArgsConstructor
public class XpGrantService {
    private final XpLedgerRepository xpLedgerRepository;
    private final WalletRepository walletRepository;
    private final ApplicationEventPublisher events;

    // 실험용 (A-3 지갑 lost update, exp/r12-race 브랜치에만 둔다)
    // atomic = 지금 코드(UPDATE ... SET xp = xp + :amount) / read-modify-write = 고치기 전 구조
    //   (findByUserId -> wallet.addXp() -> 변경 감지 UPDATE xp = 읽은 값 + amount)
    // wallet-delay-ms: 읽기(또는 원자 UPDATE) 앞에 넣는 지연. 두 모드에 똑같이 넣어 조건을 맞춘다
    @Value("${rpgym.experiment.wallet-mode:atomic}")
    private String experimentWalletMode;
    @Value("${rpgym.experiment.wallet-delay-ms:0}")
    private long experimentWalletDelayMs;
    // 실험용

    // 반환값이 비어 있으면 이미 지급된 건이다. 호출부는 이를 실패로 취급하지 않는다.
    @Transactional
    public Optional<XpLedger> grant(UUID userId, SourceType sourceType, UUID sourceId, int amount, Instant occurredAt) {
        // 1겹 -> 조용한 검사
        if (xpLedgerRepository.existsBySource(userId, sourceType, sourceId)) {
            log.info("이미 지급된 XP 라 건너뛴다. userId={} sourceType={} sourceId={}",
                    userId, sourceType, sourceId);
            return Optional.empty();
        }

        // 2겹 -> uk_xp_ledgers_source 가 경합을 최종 차단한다.
        XpLedger ledger = xpLedgerRepository.save(
                XpLedger.create(UUID.randomUUID(), userId, sourceType, sourceId, amount, occurredAt));

        addToWallet(userId, amount);

        // 구독자는 커밋이 끝난 뒤에 받는다(@TransactionalEventListener AFTER_COMMIT).
        // 그래서 구독자 쪽이 실패해도 이 트랜잭션은 되돌아가지 않는다.
        // 직접 호출로 바꾸면 redis 장애가 XP 지급을 롤백시킨다 -> 재계산 가능한 것 때문에
        // 즉 재계산 불가능한 원본을 잃는 셈이다.
        events.publishEvent(new XpGranted(userId));

        return Optional.of(ledger);
    }

    // 지갑 갱신을 "읽고 -> 더하고 -> 쓰기" 로 하지 않는다.
    // 이전 구조: findByUserId -> wallet.addXp() -> 변경 감지 UPDATE (xp = 읽은 값 + amount)
    // 파티 완료 보상은 마지막 기여자의 컨슈머 스레드가 멤버 전원의 지갑을 갱신한다.
    // 같은 순간 멤버 B 의 개인 퀘스트 완료가 B 의 파티션 스레드에서 B 의 지갑을 갱신하면,
    // 두 트랜잭션이 같은 옛 값을 읽고 각자 더해 써서 한쪽 증가분이 사라졌다 (lost update).
    // 원장은 INSERT만 진행하기 때문에 맞고 지갑만 틀리므로, 원장 합계 == 지갑 이라는 정합성 등식이 깨진다.
    // 지금 구조: UPDATE ... SET xp = xp + :amount 한 문장. 더하기를 DB 가 행 락 아래에서 한다.
    private void addToWallet(UUID userId, int amount) {
        // 실험 코드 (A-3)
        if ("read-modify-write".equals(experimentWalletMode)) {
            Optional<Wallet> found = walletRepository.findByUserId(userId);
            if (found.isPresent()) {
                Wallet wallet = found.get();
                int read = wallet.getXp();
                experimentWalletDelay();
                wallet.addXp(amount);
                walletRepository.save(wallet);
                // 로그는 영문만: 콘솔 코드페이지를 거치면 한글이 깨져 grep 이 안 된다
                log.warn("[A-3] rmw userId={} read={} amount={} write={}", userId, read, amount, wallet.getXp());
                return;
            }
        } else {
            experimentWalletDelay();
        }
        // 실험 코드

        int updated = walletRepository.addXp(userId, amount);
        if (updated == 1) {
            return;
        }

        // 지갑이 아직 없다 -- 이 유저의 첫 지급이다.
        // 두 트랜잭션이 동시에 첫 지급을 하면 둘 다 0 행을 보고 둘 다 INSERT 한다.
        // 그때는 PK(user_id) 위반으로 한쪽이 롤백되고 컨슈머가 재처리한다.
        // 재처리 때는 지갑이 있으므로 위 UPDATE 로 끝난다. 유저당 평생 한 번뿐인 경합이라 재시도 비용을 받아들인다.
        Wallet wallet = Wallet.create(userId);
        wallet.addXp(amount);
        walletRepository.save(wallet);
    }

    // 실험 코드 (A-3)
    private void experimentWalletDelay() {
        if (experimentWalletDelayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(experimentWalletDelayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
