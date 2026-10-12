# 부하 실험 — Outbox 릴레이 발행 처리량

## 무엇을 측정하나

발행기가 **초당 몇 건까지 내보낼 수 있는가**, 그리고 유입이 그 상한을 넘으면
`PENDING` 이 어떻게 쌓이는가.

**Kafka 이벤트를 밀어넣지 않고 `outbox_events` 에 직접 `PENDING` 행을 넣습니다.**
측정 대상이 컨슈머나 판정이 아니라 **릴레이(폴링 → 발행)** 이기 때문입니다.
판정 경로까지 태우면 무엇이 병목인지 섞여서 안 보입니다.

## 왜 여기가 천장인가 — 코드에 있는 구조

```java
// OutboxRelay.relayOnce()
for (OutboxEvent event : pending) {     // 최대 poll-size(100)건
    relay(event);
}

// OutboxKafkaPublisher.publish()
kafkaTemplate.send(record).get(5초);    // 한 건 보내고 응답을 기다린다
```

```
// OutboxPublishScheduler
@Scheduled(fixedDelayString = "1000")   // 이전 라운드가 끝난 뒤 1초 쉬고 다음 라운드
```

상한은 이렇게 정해집니다.

```
처리량 =  poll-size / (라운드 소요시간 + poll-interval)
       =  100 / (100건 x 왕복시간 + 1초)
```

**레버가 둘입니다** — `poll-size` 와 라운드 소요시간(= 동기 대기).
측정 전에는 둘 중 어느 쪽이 지배적인지 모릅니다. **그걸 알아내는 것이 이 실험입니다.**

## 실행 순서

**사전 조건** — postgres · kafka · game-service 가 떠 있어야 합니다.
`game.events` 구독자는 없어도 됩니다 (발행만 확인합니다).

```powershell
# 1) 창 A — 시드 5,000건 주입
Get-Content .\01_seed.sql | docker exec -i rp-gym-postgres-game psql -U postgres -d game_db

# 2) 창 B — 즉시 관측 시작 (1초마다 샘플)
powershell -ExecutionPolicy Bypass -File .\02_watch.ps1
```

관측 창이 이렇게 찍습니다.

```
elapsed_sec,pending,published,published_delta
1.0,4900,100,100
2.1,4800,200,100      <- published_delta 가 곧 초당 처리량
...
```

`pending` 이 0이 되면 자동으로 멈추고 **총 소요시간과 평균 처리량**을 출력합니다.

## 기록할 것

| 항목 | 값 |
|---|---|
| 시드 건수 | 5,000 |
| 총 소진 시간 | ____ 초 |
| 평균 처리량 | ____ 건/초 |
| 라운드당 처리 시간 | game-service 로그의 폴링 간격으로 확인 |
| poll-size / poll-interval | 100 / 1000ms |

**실행 환경도 같이 적습니다** (카드의 테스트 조건에 그대로 들어갑니다).

```powershell
docker exec rp-gym-postgres-game psql -U postgres -c "SELECT version();"
docker stats --no-stream rp-gym-postgres-game rp-gym-kafka
# 그리고 game-service 를 띄운 JVM 의 힙 설정, 호스트 CPU/메모리
```

## 다음 단계 — 고친 뒤 같은 실험을 반복

레버 둘을 하나씩 바꿔가며 같은 5,000건을 재주입합니다.

```
(1) 지금 그대로                    기준선
(2) poll-size 만 100 -> 500        폴링 횟수가 병목인지 확인
(3) 발행을 배치 비동기로            라운드 소요시간이 병목인지 확인
    (전부 send 한 뒤 future 를 순서대로 기다린다)
(4) (2)+(3)
```

**(3)을 할 때 답해야 하는 것** — 지금은 "한 건 실패하면 라운드 중단" 입니다.
다 보내놓고 나중에 기다리면 **중간에 실패했을 때 이미 나간 뒤 건들을 어떻게 볼 것인가**.
이 답이 카드의 「해결과정」이 됩니다.

> 순서 보장은 안 깨집니다. `enable.idempotence: true` + `max.in.flight: 5` 라
> 프로듀서가 파티션 내 순서를 유지합니다. (application.yml 에 이미 있습니다)

## 정리

```powershell
Get-Content .\99_cleanup.sql | docker exec -i rp-gym-postgres-game psql -U postgres -d game_db
```
