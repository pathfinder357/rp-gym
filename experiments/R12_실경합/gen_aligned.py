"""R-12 실경합 실험 — 정렬 주입 데이터 생성기 (2026-10-11)

목표: 같은 파티 멤버의 이벤트를 서로 다른 스레드가 거의 같은 순간에 처리하게 만든다.

방법: Kafka 기본 파티셔너와 같은 계산(murmur2(key) & 0x7fffffff % 파티션 수)으로 userId 를 고른다.
      파티 p 의 멤버 i 는 파티션 (p + i) % 3 으로 보낸다(순환 배치).
      멤버가 4명이면 네 번째는 첫 번째와 같은 파티션에 들어가고, 그 "겹치는 파티션"이 파티마다 돌아간다.
      그래서 파티션별 이벤트 수가 거의 같아지고(134/133/133), 스레드끼리 크게 어긋나지 않는다.

기본값: 멤버 4명(실제 최대 인원) · 목표 3000.
  파티션이 3개라 한 파티에서 동시에 열릴 수 있는 트랜잭션은 최대 3개다(4명 동시는 구조상 불가능).
  목표 4000(전원 필요)이면 완료시키는 이벤트가 같은 파티션의 뒤 순번 하나가 되기 쉬워 완료 경합이 약하다.
  목표 3000이면 서로 다른 파티션의 세 명이 완료를 다투고, 네 번째 기여는 "완료 직후 도착한 기여"가 된다
  (카운터에 상태 조건을 걸지 않은 이유 = 트레이드오프 A 를 실제 파이프라인에서 확인).
  이상화 시뮬레이션(모든 트랜잭션 길이 같음, 동시 시작) 완료 순간 겹침:
    4명·목표3000·순환 100/100, 4명·목표4000·순환 66/100, 4명·목표4000·고정 0/100, 3명·목표3000 100/100

식별자 앞자리는 예전 실험과 같다 -> 99_cleanup.sql 을 그대로 쓴다.
  파티 11111111 / 파티 퀘스트 22222222 / 유저 33333333 / 멤버 행 44444444 / 이벤트 55555555

사용: python gen_aligned.py [--parties 100] [--members 4] [--target 3000] [--partitions 3] [--personal]

--personal (A-3 · A-4, 2026-10-11): 멤버마다 개인 퀘스트(기준 1000 · 목표 500 · 보상 10)를 하나씩 더 넣는다.
  같은 이벤트(누적 2000)가 개인 퀘스트도 완료시킨다. 그래서 한 유저의 지갑을 두 트랜잭션이 갱신한다:
  자기 스레드의 개인 지급(+10)과, 파티를 완료시킨 다른 멤버 스레드의 파티 지급(+30).
  지갑은 xp 0 으로 미리 만든다(첫 지급의 INSERT 경합이 아니라 UPDATE 경합을 보려고).
  식별자: 개인 퀘스트 66666666 / 제안 77777777
출력: 01_seed_aligned.sql, 02_events_aligned.template.txt (같은 폴더)
"""
import argparse
import os

SEED = 0x9747B28C
M = 0x5BD1E995
MASK = 0xFFFFFFFF


def murmur2(data: bytes) -> int:
    """org.apache.kafka.common.utils.Utils.murmur2 와 같은 결과(부호 있는 32비트)"""
    length = len(data)
    h = (SEED ^ length) & MASK
    for i in range(length // 4):
        i4 = i * 4
        k = data[i4] | (data[i4 + 1] << 8) | (data[i4 + 2] << 16) | (data[i4 + 3] << 24)
        k = (k * M) & MASK
        k ^= k >> 24
        k = (k * M) & MASK
        h = (h * M) & MASK
        h ^= k
    rest = length % 4
    base = length & ~3
    if rest == 3:
        h ^= data[base + 2] << 16
    if rest >= 2:
        h ^= data[base + 1] << 8
    if rest >= 1:
        h ^= data[base]
        h = (h * M) & MASK
    h ^= h >> 13
    h = (h * M) & MASK
    h ^= h >> 15
    return h - (1 << 32) if h & 0x80000000 else h


def partition_of(key: str, partitions: int) -> int:
    return (murmur2(key.encode("utf-8")) & 0x7FFFFFFF) % partitions


def self_test():
    # Kafka UtilsTest.testMurmur2 의 기대값
    cases = {
        b"21": -973932308,
        b"foobar": -790332482,
        b"a-little-bit-long-string": -985981536,
        b"a-little-bit-longer-string": -1486304829,
        b"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
        b"abc": 479470107,
    }
    for data, want in cases.items():
        got = murmur2(data)
        assert got == want, f"murmur2({data!r}) = {got}, Kafka 기대값 {want}"


def uid(prefix: str, party: int, member: int) -> str:
    return f"{prefix}-0000-4000-8000-{party:09d}{member:03d}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--parties", type=int, default=100)
    ap.add_argument("--members", type=int, default=4)
    ap.add_argument("--target", type=int, default=3000)
    ap.add_argument("--partitions", type=int, default=3)
    ap.add_argument("--personal", action="store_true")
    a = ap.parse_args()
    if not 2 <= a.members <= 4:
        raise SystemExit("파티 인원은 2~4명")
    if a.target % 1000 or not 1000 <= a.target <= a.members * 1000:
        raise SystemExit("목표는 1000 단위, 1000 ~ 멤버 수 × 1000")
    self_test()

    here = os.path.dirname(os.path.abspath(__file__))
    plan = []  # (party, slot, memberNo, userId) — 파티 순서, 그 안에서 멤버 순서
    for p in range(1, a.parties + 1):
        used = set()
        for i in range(a.members):
            slot = (p + i) % a.partitions  # 순환 배치
            for j in range(1, 1000):
                if j in used:
                    continue
                user = uid("33333333", p, j)
                if partition_of(user, a.partitions) == slot:
                    used.add(j)
                    plan.append((p, slot, j, user))
                    break
            else:
                raise SystemExit(f"파티 {p} 멤버 {i} 에 맞는 userId 를 못 찾음")

    target = a.target
    late = a.members - target // 1000
    n = a.parties * a.members
    rows_users = ",\n".join(f"    ('{u}'::uuid, {p}, {j})" for p, _, j, u in plan)
    seed = f"""-- ============================================================
-- R-12 실경합 실험 시드 (gen_aligned.py 가 만듦 · 손으로 고치지 말 것)
-- 파티 {a.parties}개 × 멤버 {a.members}명, 파티 p 의 멤버 i 는 파티션 (p + i) % {a.partitions} 로 가는 userId
-- 기준값 1000 · 이벤트 누적값 2000 -> 한 사람 기여 1000, 목표 {target} -> {target // 1000}명째 기여에서 완료
-- 완료 뒤에 도착하는 기여: 파티당 {late}건 (카운터에는 더해지고, 보상은 다시 나가지 않아야 한다)
--
-- 파티 퀘스트 expired_at 은 오늘(KST) 23:59:59 다. PartyQuestCreateService 와 같다.
-- R-9 이후 파티 퀘스트 날짜 = expiredAt 의 KST 날짜라서, 예전 시드처럼 now() + 6시간으로 넣으면
-- KST 18시 이후에는 퀘스트 날짜가 내일이 되어 모든 이벤트가 OTHER_DAY 로 무시된다.
-- ============================================================

DO $$
BEGIN
    IF (now() AT TIME ZONE 'Asia/Seoul')::time > time '23:30' THEN
        RAISE EXCEPTION 'KST 23:30 이후다. 자정을 넘기면 퀘스트 날짜와 이벤트 날짜가 갈린다. 내일 다시 하자';
    END IF;
END $$;

BEGIN;

CREATE TEMP TABLE r12_members (user_id uuid, party int, member int) ON COMMIT DROP;
INSERT INTO r12_members VALUES
{rows_users};

INSERT INTO game_service.parties
    (id, party_name, owner_id, status, visibility, metric, max_member, current_member,
     matching_deadline_at, ends_at)
SELECT ('11111111-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       'R12 실험파티 ' || i,
       (SELECT user_id FROM r12_members WHERE party = i ORDER BY member LIMIT 1),
       'ACTIVE', 'PRIVATE', 'STEPS', {a.members}, {a.members},
       now() - interval '1 hour', now() + interval '6 days'
  FROM generate_series(1, {a.parties}) AS i;

INSERT INTO game_service.user_latest_snapshots
    (user_id, activity_date, measured_at, steps, active_minutes, active_calories)
SELECT user_id, (now() AT TIME ZONE 'Asia/Seoul')::date, now() - interval '1 hour', 1000, 0, 0
  FROM r12_members;

INSERT INTO game_service.party_quests
    (party_quest_id, party_id, title, metric, target_val, current_val, status,
     reward_xp, started_at, expired_at)
SELECT ('22222222-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('11111111-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       'R12 실험 파티 퀘스트 ' || i,
       'STEPS', {target}, 0, 'ACTIVE', 30,
       now() - interval '1 hour',
       ((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'
  FROM generate_series(1, {a.parties}) AS i;

INSERT INTO game_service.party_quest_members
    (party_quest_member_id, party_quest_id, user_id, baseline_val, contributed_val,
     last_applied_measured_at)
SELECT ('44444444-0000-4000-8000-' || lpad(party::text, 9, '0') || lpad(member::text, 3, '0'))::uuid,
       ('22222222-0000-4000-8000-' || lpad(party::text, 12, '0'))::uuid,
       user_id, 1000, 0, NULL
  FROM r12_members;

__PERSONAL__COMMIT;

SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE '66666666-0000-4000-8000-%') AS 개인퀘스트,
       (SELECT count(*) FROM game_service.parties WHERE id::text LIKE '11111111-0000-4000-8000-%') AS 파티,
       (SELECT count(*) FROM game_service.party_quests WHERE party_quest_id::text LIKE '22222222-0000-4000-8000-%') AS 파티퀘스트,
       (SELECT count(*) FROM game_service.party_quest_members WHERE party_quest_id::text LIKE '22222222-0000-4000-8000-%') AS 멤버,
       (SELECT count(*) FROM game_service.user_latest_snapshots WHERE user_id::text LIKE '33333333-0000-4000-8000-%') AS 스냅샷;
-- {a.parties} / {a.parties} / {n} / {n} 이어야 한다
"""
    personal = ""
    if a.personal:
        personal = """-- (A-3 · A-4) 멤버마다 개인 퀘스트. 같은 이벤트가 개인(+10)과 파티(+30)를 함께 지급한다
INSERT INTO game_service.quests
    (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val, baseline_measured_at,
     last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at)
SELECT ('66666666-0000-4000-8000-' || lpad(party::text, 9, '0') || lpad(member::text, 3, '0'))::uuid,
       user_id,
       ('77777777-0000-4000-8000-' || lpad(party::text, 9, '0') || lpad(member::text, 3, '0'))::uuid,
       'A3 personal quest', 'STEPS', 500, 1000, now() - interval '1 hour', NULL, NULL, 'ACTIVE', 10,
       ((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'
  FROM r12_members;

INSERT INTO game_service.wallets (user_id, xp)
SELECT user_id, 0 FROM r12_members;

"""
    seed = seed.replace("__PERSONAL__", personal)
    with open(os.path.join(here, "01_seed_aligned.sql"), "w", encoding="utf-8", newline="\n") as f:
        f.write(seed)

    lines = []
    for p, _, j, u in plan:
        eid = uid("55555555", p, j)
        lines.append(
            f'{u}|{{"eventId":"{eid}","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"{u}",'
            f'"data":{{"activityDate":"__DATE__","measuredAt":"__MEASURED_AT__",'
            f'"cumulative":{{"steps":2000,"activeMinutes":0,"activeCalories":0}}}}}}'
        )
    with open(os.path.join(here, "02_events_aligned.template.txt"), "w", encoding="utf-8", newline="\n") as f:
        f.write("\n".join(lines) + "\n")

    per = [0] * a.partitions
    for _, slot, _, u in plan:
        assert partition_of(u, a.partitions) == slot
        per[slot] += 1
    print("murmur2 자가 검사 통과 (Kafka UtilsTest 기대값 6건)")
    print(f"파티 {a.parties}개 × 멤버 {a.members}명 = 이벤트 {n}건, 목표 {target} (완료 뒤 도착 기여 파티당 {late}건)")
    print("파티션별 이벤트 수:", {i: c for i, c in enumerate(per)}, "-> 실행 때 오프셋 증가량이 이와 같아야 한다")
    print(f"검증 기대값: 1) {a.parties}/0/{a.parties}  2) {n}건 · {n * 30}  3) 0행  4) 0행  5) {a.parties}  6) 0행  참고) 1000 × {n}")
    if a.personal:
        print(f"개인 퀘스트: {n}개 (기준 1000 · 목표 500 · 보상 10) -> 지갑 기대값 = 유저마다 10 + 30 = 40, 총 {n * 40}")
    print("만든 파일: 01_seed_aligned.sql, 02_events_aligned.template.txt")


if __name__ == "__main__":
    main()
