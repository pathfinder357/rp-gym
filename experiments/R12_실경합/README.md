# R-12 실경합 실험 — 같은 파티 멤버를 정말 "동시에" 처리시키기

> 리펙토링.md `★ 앞으로 할 일` G절의 A-1. 2026-10-11 작성.
> 2026-09-22 실험(400건)은 결과는 맞았지만, 서로 다른 스레드의 파티 트랜잭션이 실제로 겹친 파티가 100개 중 3개뿐이었다.
> 이번 목표는 **겹친 파티가 과반인 조건**에서 완료 100/100 · 원장 정확 · 정합성 위반 0을 보이는 것이다.

## 무엇을 바꿨나 (지난 실험과 다른 점 세 가지)

| | 2026-09-22 | 이번 (기본값) |
|---|---|---|
| 파티 구성 | 4명 · 목표 4000 (전원 필요) | **4명 · 목표 3000** (세 명째에서 완료, 네 번째는 완료 직후 도착) |
| 파티션 배치 | 키가 우연히 흩어짐 → 파티션별 116 · 136 · 148건 | **순환 배치**: 파티 p 의 멤버 i 를 파티션 (p + i) % 3 으로. 4명이면 네 번째는 첫 번째와 같은 파티션이고, 그 겹치는 파티션이 파티마다 돌아간다 → 133 · 134 · 133건 |
| 트랜잭션 길이 | 13ms 안팎이라 거의 겹치지 않음 | **실험용 지연**(기본 200ms)으로 겹침 구간을 넓힘. 판정 로직은 그대로 |

### 왜 4명 · 목표 3000인가
파티션이 3개라 **한 파티에서 동시에 열릴 수 있는 트랜잭션은 최대 3개**다. 네 명 중 두 명은 반드시 같은 파티션(같은 스레드)이라 차례로 처리된다. 즉 "네 명이 동시에 마지막 기여"는 이 배포 구조에서 일어날 수 없다(예전 README · 포트폴리오 표현을 고칠 때 주의).

목표를 4000(전원 필요)으로 두면 완료시키는 이벤트가 겹치는 파티션의 뒤 순번 하나가 되기 쉬워, 완료 순간에 경쟁하는 상대가 없다. 이상화 시뮬레이션(트랜잭션 길이 모두 같음, 동시 시작) 결과:

| 설계 | 파티션별 건수 | 완료 순간 겹침 | 평균 최대 동시 |
|---|---|---|---|
| 4명 · 목표 4000 · 겹치는 파티션 고정 (예전과 비슷한 상황) | 200 · 100 · 100 | **0/100** | 2.01 |
| 4명 · 목표 4000 · 순환 | 134 · 133 · 133 | 66/100 | 2.67 |
| **4명 · 목표 3000 · 순환 (기본값)** | 134 · 133 · 133 | **100/100** | 2.67 |
| 3명 · 목표 3000 | 100 · 100 · 100 | 100/100 | 3.00 |

목표 3000이면 서로 다른 파티션의 세 명이 완료를 다투고, 네 번째 기여는 **완료 직후 도착한 기여**가 된다. 이건 카운터 UPDATE에 상태 조건을 걸지 않은 이유(포트폴리오 파티 카드의 트레이드오프 A)다. 지금은 `PartyQuestCounterRaceTest`(Testcontainers)로만 보였는데, 실제 Kafka 파이프라인에서 100번 확인하게 된다. 기대값: 완료 후에도 카운터에 더해져 **카운터 = 멤버 기여 합 = 4000**, 보상은 완료 때 한 번 **멤버 4명 모두**에게.

다른 설계로 돌리려면: `python gen_aligned.py --members 4 --target 4000` (전원 필요, 경합 약함) / `--members 3 --target 3000` (세 명 모두 동시, 가장 순수한 경합)

### 그 밖에
- 정렬: `gen_aligned.py`가 Kafka 기본 파티셔너와 같은 계산(`murmur2(key) & 0x7fffffff % 3`)으로 userId를 고른다. 구현은 Kafka 자체 테스트 기대값 6건으로 자가 검사한다. 예전 400건의 userId를 같은 계산에 넣으면 117 · 135 · 148이 나와, 로그에서 센 116 · 136 · 148과 한 건 차이다(그때 기동 중 리밸런싱 등으로 한 건이 다른 스레드에서 처리된 것으로 보임). 그래서 실행 때 **파티션별 오프셋 증가량을 직접 확인하는 단계**를 넣었다.
- 지연을 넣는 위치: 자기 멤버 행을 갱신한 뒤, **공유 카운터를 올리기 직전**. 이때 각 스레드는 자기 멤버 행만 잡고 있어서 서로 막지 않고 함께 기다렸다가 카운터(경합 지점)로 몰린다.
- 결과를 말할 때 반드시 붙일 조건: "실험용 지연 200ms로 겹침 구간을 넓혔다". 실제 트래픽에서 같은 파티 멤버 세 명이 수 ms 안에 동시에 들어올 일은 드물다. 이 실험은 "이만큼 몰려도 맞다"는 스트레스 조건이다.

## 파일

| 파일 | 하는 일 |
|---|---|
| `gen_aligned.py` | 시드 SQL과 이벤트 템플릿을 만든다 (`python gen_aligned.py`) |
| `01_seed_aligned.sql` | 파티 100 × 멤버 4 · 목표 3000 (기본값). 파티 퀘스트 기한은 **오늘(KST) 23:59:59** |
| `02_events_aligned.template.txt` | 이벤트 400건, 파티 순서 → 그 안에서 멤버 순서 |
| `03_verify_aligned.sql` | 정합성 6가지 (파티별 멤버 수와 비교하므로 멤버 수가 바뀌어도 그대로 쓴다) |
| `overlap_report.py` | 로그로 "정말 동시였나"를 센다: 최대 동시 수 · 완료 순간 겹침 · 완료 뒤 커밋된 기여 · 잠금 대기 |
| 상위 폴더 `99_cleanup.sql` | 실험 데이터 정리 (식별자 앞자리가 같아서 그대로 쓴다) |

> ⚠ 예전 `01_seed.sql`은 기한을 `now() + 6시간`으로 넣는다. R-9 이후 파티 퀘스트 날짜는 기한의 KST 날짜라서, **KST 18시 이후에 그 시드를 쓰면 모든 이벤트가 OTHER_DAY로 무시되어 완료 0건**이 나온다. 이 폴더의 시드를 쓴다. 그리고 KST 23:30 이후에는 시드가 일부러 실패한다(자정을 넘기면 날짜가 갈린다).

---

## 준비 (한 번만)

### 1. 실험 브랜치를 딴다
실험용 지연 코드는 커밋할 코드가 아니다. **G절 0번(전체 테스트 · 커밋)을 먼저 끝내고** 브랜치를 딴다. 그래야 R-14~R-20 변경과 섞이지 않는다.

```powershell
cd C:\workspace\health_service
git switch -c exp/r12-race
```

### 2. 실험용 지연을 넣는다
`game-service/src/main/java/com/workoutdone/rpgym/game/quest/application/PartyQuestProgressService.java`

필드 (클래스 안 아무 곳, `@RequiredArgsConstructor`는 final 필드만 생성자에 넣으므로 그대로 둔다):
```java
import org.springframework.beans.factory.annotation.Value;

    // (R-12 실험 전용) 겹침 구간을 넓히는 지연. 기본 0 = 꺼짐. exp/r12-race 브랜치에만 둔다
    @Value("${rpgym.experiment.party-tx-delay-ms:0}")
    private long experimentDelayMs;
```

`apply()` 안, `if (counterDelta == 0) { return ...; }` 바로 아래 · `// 4. 공유 카운터를 올린다` 바로 위:
```java
        if (experimentDelayMs > 0) {
            try {
                Thread.sleep(experimentDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
```

### 3. 데이터를 만든다
```powershell
cd C:\workspace\docs\mvp_이후설계\실험\R12_실경합
python gen_aligned.py
```
`파티션별 이벤트 수: {0: 133, 1: 134, 2: 133}`과 검증 기대값이 출력된다. 이 두 줄을 결과 기록에 옮겨 둔다.

---

## 실행 (회차마다 전부)

회차 이름을 정한다. 예: `run1` (지연 200ms, 스레드 3). 로그 파일 이름에 그대로 쓴다. **`Tee-Object`는 덮어쓰므로 회차마다 이름을 바꾼다.**

### 1. 인프라를 띄우고 파티션 수를 확인한다
```powershell
cd C:\workspace\health_service
docker compose -f docker-compose-develop.yml up -d postgres-game kafka redis
docker exec rp-gym-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic health.events
```
`PartitionCount: 3`이어야 한다. 1이면 스레드가 하나뿐이라 실험이 성립하지 않는다.

### 2. 지난 회차 데이터를 지운다
```powershell
cd C:\workspace\docs\mvp_이후설계\실험
docker cp 99_cleanup.sql rp-gym-postgres-game:/tmp/99_cleanup.sql
docker exec rp-gym-postgres-game psql -U postgres -d game_db -f /tmp/99_cleanup.sql
```

### 3. (선택) PostgreSQL 잠금 대기 로그를 켠다
20ms 넘게 행 잠금을 기다리면 PostgreSQL이 로그를 남긴다. "같은 행을 두고 실제로 줄을 섰다"는 DB 쪽 증거다. 0건이어도 실험 실패는 아니다(지연 위치상 카운터에 도착하는 시각이 ms 단위로 갈리면 기다리지 않는다).
```powershell
docker exec rp-gym-postgres-game psql -U postgres -d game_db -c "ALTER SYSTEM SET log_lock_waits = on" -c "ALTER SYSTEM SET deadlock_timeout = '20ms'" -c "SELECT pg_reload_conf()"
```

### 4. 앱을 띄운다 (지연 200ms)
```powershell
cd C:\workspace\health_service
.\gradlew.bat :game-service:bootRun --args="--rpgym.experiment.party-tx-delay-ms=200" 2>&1 | Tee-Object -FilePath r12-run1.log
```
콘솔에 `Started`가 뜨고, **`partitions assigned`가 세 번(파티션 0 · 1 · 2가 각각 다른 스레드)** 뜰 때까지 기다린다. 앱을 먼저 띄워 컨슈머가 자리를 잡은 뒤에 이벤트를 넣어야 한다. 이벤트를 먼저 쌓아 두고 앱을 띄우면, 기동 중 리밸런싱 때 한 스레드가 여러 파티션을 잠깐 잡고 먼저 처리해 버린다.

### 5. 시드를 넣는다 (새 터미널)
```powershell
cd C:\workspace\docs\mvp_이후설계\실험\R12_실경합
docker cp 01_seed_aligned.sql rp-gym-postgres-game:/tmp/01_seed_aligned.sql
docker exec rp-gym-postgres-game psql -U postgres -d game_db -f /tmp/01_seed_aligned.sql
```
마지막 줄이 `100 | 100 | 400 | 400`이어야 한다.

### 6. 이벤트 파일을 지금 시각으로 만든다 (회차마다 새로)
시드의 스냅샷 측정 시각(1시간 전)보다 뒤여야 워터마크에 안 걸린다. 지난 회차 파일을 다시 쓰면 400건이 전부 무시된다.
```powershell
$measuredAt = [DateTimeOffset]::Now.ToString("yyyy-MM-ddTHH:mm:sszzz")
$date       = [DateTime]::Now.ToString("yyyy-MM-dd")
(Get-Content 02_events_aligned.template.txt -Encoding UTF8) `
    -replace '__MEASURED_AT__', $measuredAt `
    -replace '__DATE__', $date |
    Set-Content 02_events_aligned.txt -Encoding UTF8
Get-Content 02_events_aligned.txt -TotalCount 1
```
`__`가 남아 있으면 치환이 안 된 것이다.

### 7. 파티션별 오프셋을 적어 두고, 400건을 쏘고, 다시 본다
```powershell
docker exec rp-gym-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic health.events

docker cp 02_events_aligned.txt rp-gym-kafka:/tmp/events.txt
docker exec rp-gym-kafka sh -c "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic health.events --property parse.key=true --property key.separator='|' < /tmp/events.txt"

docker exec rp-gym-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic health.events
```
**파티션별 증가량이 `gen_aligned.py`가 출력한 값(133 · 134 · 133)과 정확히 같아야** 한다. 아니면 파티셔너 계산이 실제와 다르다는 뜻이니 여기서 멈추고 결과를 가져온다.

### 8. 처리가 끝날 때까지 기다린다
스레드 하나가 약 134건 × 200ms ≈ 27초. 로그가 조용해지면 끝난 것이다.

### 9. 검증한다
```powershell
docker cp 03_verify_aligned.sql rp-gym-postgres-game:/tmp/03_verify_aligned.sql
docker exec rp-gym-postgres-game psql -U postgres -d game_db -f /tmp/03_verify_aligned.sql
```
기대값: `1) 100 / 0 / 100  2) 400건 · 12000  3) 0행  4) 0행  5) 100  6) 0행  참고) 1000 × 400`
4번이 0행이라는 것은 완료 뒤에 도착한 네 번째 기여까지 카운터에 더해졌다는 뜻이다(카운터 = 기여 합 = 4000).

### 10. (3번을 했다면) PostgreSQL 로그를 저장한다
PowerShell 5.1의 `>`는 UTF-16으로 쓰고 stderr를 오류 객체로 감싸므로 `cmd`로 넘긴다.
```powershell
cd C:\workspace\health_service
cmd /c "docker logs --since 15m rp-gym-postgres-game > r12-run1-pg.log 2>&1"
```

### 11. 앱을 멈추고(Ctrl+C) 분석한다
```powershell
cd C:\workspace\health_service
python C:\workspace\docs\mvp_이후설계\실험\R12_실경합\overlap_report.py r12-run1.log r12-run1-pg.log
```

**판정**
- `완료 순간 겹침`이 과반(50/100 이상)이고 9번 검증이 전부 기대값이면 → "완료를 두고 실제로 경합한 조건에서 맞았다"고 말할 수 있다
- `완료 뒤 커밋된 기여`가 100에 가까우면 → "완료 직후 도착한 기여도 카운터에 들어갔다(트레이드오프 A)"를 실제 파이프라인에서 확인한 것이다
- `완료 순간 겹침`이 낮으면 → 지연을 500ms로 올려 `run2`로 다시 한다(4번의 `--args`만 바꾼다)
- 완료가 100보다 적으면 → 먼저 6번(이벤트 시각) · 시드의 날짜(KST 23:30 전인지)를 의심한다. 로그에서 `OTHER_DAY` · `STALE`를 찾아본다

### 12. 대조군 (스레드 1개)
2번(정리) → 4번을 아래처럼 → 5~11번을 그대로. 로그 이름은 `r12-c1`.
```powershell
.\gradlew.bat :game-service:bootRun --args="--rpgym.experiment.party-tx-delay-ms=200 --spring.kafka.listener.concurrency=1" 2>&1 | Tee-Object -FilePath r12-c1.log
```
기대: 검증은 run1과 **같고**, `최대 동시 1개` · `완료 순간 겹침 0/100` · 잠금 대기 0건. 처리 시간은 400건 × 200ms ≈ 80초. "도구가 겹침을 제대로 센다"는 것과 "결과가 같다"를 함께 보인다.

### 13. 되돌린다
```powershell
docker exec rp-gym-postgres-game psql -U postgres -d game_db -c "ALTER SYSTEM RESET log_lock_waits" -c "ALTER SYSTEM RESET deadlock_timeout" -c "SELECT pg_reload_conf()"
cd C:\workspace\docs\mvp_이후설계\실험
docker exec rp-gym-postgres-game psql -U postgres -d game_db -f /tmp/99_cleanup.sql
```
지연 코드는 `exp/r12-race` 브랜치에만 남긴다. A-2(반대 실험)도 이 브랜치에서 이어 한다.

---

## 결과 기록 (회차마다 한 줄)

| 회차 | 설계 | 지연 | 스레드 | 파티션별 증가 | 최대 동시 분포 | 완료 순간 겹침 | 완료 뒤 커밋 | 잠금 대기(party_quests) | 완료 | 원장 | 위반(3·4·6) |
|---|---|---|---|---|---|---|---|---|---|---|---|
| run1 (2026-10-11) | 4명 · 3000 | 200ms | 3 | 134 · 133 · 133 (스레드별) | 2개 99 · 3개 1 | **99/100** | 77/100 | 미측정 | 100/100 | 400 · 12000 | 0 · 0 · 0 |
| c1 (2026-10-11) | 4명 · 3000 | 200ms | 1 | 400 (스레드 1) | 1개 100 | **0/100** | 100/100 | 미측정 | 100/100 | 400 · 12000 | 0 · 0 · 0 |

| A-2 (2026-10-11) | 4명 · 3000 · **naive-claim** | 200ms | 3 | 134 · 133 · 133 | 1개 34 · 2개 32 · 3개 34 | (완료 로그 기준이라 비교 안 함) | | 미측정 | 100/100 (상태만) | **288 · 8640** | 3번 **28파티** · 4 · 6 = 0 |

A-2 반대 실험(조건부 UPDATE 결과를 무시하고 트랜잭션 시작 때 읽은 값으로 자바에서 완료 판단): **28파티는 상태가 COMPLETED 인데 보상 0건(112명분 XP 누락)**, **9파티는 두 스레드가 동시에 "내가 완료시켰다"고 판단** → 두 번째 지급은 원장 조용한 검사 + Outbox 유니크 인덱스(중복 키 9건)가 막아 원장 중복은 0. 완료 로그 81줄 = 실제 커밋 72 + 롤백된 중복 9. 같은 조건에서 정상 코드(run1)는 원장 400 · 위반 0 → **조건부 UPDATE 하나가 보상 누락 28파티와 중복 판정 9파티를 막고 있었다.** README 7-1의 "원장 1600" 예측은 틀렸다: 실제 깨짐은 중복이 아니라 누락이 더 컸다.

완료 뒤 기여의 두 갈래: run1 카운터 4000 = 84파티(네 번째가 완료 전에 시작해 더해짐, 트레이드오프 A) · 3000 = 16파티(완료 뒤 시작해 무시) / c1 카운터 3000 = 100파티(차례 처리라 네 번째는 항상 완료 뒤 시작 → 전부 무시). **트레이드오프 A 상황은 동시 처리일 때만 생긴다**는 것이 대조군으로 확인됨

이 표와 `overlap_report.py` 출력, 로그 파일을 저장소에 올리면 포트폴리오 1장 파티 카드 · 근거 페이지 `evidence/party-concurrency-400.html` · 이력서 파티 사례 결과 줄 · 질문 은행 A 6~10을 고친다.

## 다음: A-2 반대 실험을 위해 알아 둘 것
README 7-1(상위 폴더)의 "claimCompletion 반환값 확인을 없애면 원장이 1600건" 예측은 지금 코드에서 그대로 나오지 않을 가능성이 크다. 원장은 조용한 검사 + 유니크 제약이, 완료 이벤트는 outbox 유니크 인덱스 `(aggregate_type, aggregate_id, event_type)`가 뒤에서 막는다. 그래서 반대 실험은 원장 수가 아니라 **완료 판정 횟수(완료 로그 줄 수) · 유니크 위반 · DLT 건수**로 판정하도록 설계한다.

## 실행기 2단계 결과 (2026-10-11, `run_exp.ps1`)

| 실험 | 실행 | 결과 | 판정 |
|---|---|---|---|
| A-5 늦은 이벤트 | `-Exp A-5` | 어제만 온 개인 25 → ACTIVE 25 · 보상 0 / 어제 8000 뒤 오늘 6000 → 완료 25 · 누적 6000 · 보상 25 / 파티 25(어제 8000 먼저) → 카운터 4000 · 기여 1000×100 · 보상 100 / 제안 31분 → 0 · 29분 → 10 | 기대값 전부 일치 |
| A-7 정합성 대조 | `-Exp A-7` | 처리 뒤 전부 0 → 원장 1행 삭제 · 지갑 +100 → `ledger_wallet` 2 · `party_reward` 1 · `ranking` 1 · 나머지 0, ERROR 샘플 8줄 | 기대값 정확히 일치 |
| A-9 Redis 없이 | `-Exp A-9` | UnexpectedRollbackException 0(09-22 고치기 전 400) · characters 400 · 실패 카운터 redis 400 · listener 0 | 통과 |
| A-11 Redis 정지 중 지급 | `-Exp A-11` | 실패 카운터 redis 400 → 복구 후 `ranking` 525 → 재구축 `rebuilt 525` → 전부 0 | 통과 + **약점 발견**: Redis 정지 중 대조 작업 통째로 실패, `last_success` 0 (리펙토링 R-21) |
