"""Kafka 3대 기동 · Kafka 3대 장애 Kafka 3대 (Kafka 3대 구성) — 데이터 생성기 (2026-10-12)

Kafka 3대 기동: 브로커 3대 · 토픽 6개(복제 3, min.insync 2)
Kafka 3대 장애 ① 브로커 1대(kafka-2) 정지 중 이벤트 100건 -> 완료 100 · Outbox 100 발행 · 발행 실패 0 · DLT 0 · 대조 0
     ② 브로커 2대(kafka-2, kafka-3) 정지 -> ISR 1 < min.insync 2 -> acks=all 발행 거부(NotEnoughReplicas)
        이벤트 20건(주입 도구는 acks=1 로 넣는다 -- Health 의 실제 설정이 아니라 입력을 넣기 위한 편의)
        -> Game 은 처리 · 완료하지만 Outbox 발행이 막혀 PENDING 으로 쌓임 -> Outbox 최장 대기 알림
        -> kafka-2 재기동(ISR 2) -> 쌓인 것 발행 · 유실 0 · 중복 건수
     ③ 비교군(브로커 1대)은 DB · Kafka 정지 K(Kafka 95초 정지)로 갈음
식별자 앞자리 c1: 유저 c1000001 / 퀘스트 c1000002 / 이벤트 c1000009.  ① 유저 1~100  ② 유저 201~220
"""
import os

here = os.path.dirname(os.path.abspath(__file__))
U = lambda pfx, n: f"{pfx}-0000-4000-8000-{n:012d}"
TODAY_END = "((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'"
P1 = list(range(1, 101))
P2 = list(range(201, 221))


def ev(i):
    u = U("c1000001", i)
    return (f'{u}|{{"eventId":"{U("c1000009", i)}","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"{u}",'
            f'"data":{{"activityDate":"__TODAY__","measuredAt":"__NOW__","cumulative":{{"steps":2000,"activeMinutes":0,"activeCalories":0}}}}}}')


users = P1 + P2
seed = f"""-- Kafka 3대 장애 시드 (gen.py 가 만듦)
DO $$ BEGIN
  IF (now() AT TIME ZONE 'Asia/Seoul')::time > time '23:30' THEN RAISE EXCEPTION '자정 근처다. 23:30 전에 하자'; END IF;
END $$;
BEGIN;
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
SELECT ('c1000002-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('c1000001-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       gen_random_uuid(), 'C20 personal ' || i, 'STEPS', 500, 1000,
       now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, {TODAY_END}, 0
  FROM unnest(ARRAY[{",".join(map(str, users))}]) AS i;
COMMIT;
"""


def group_sql(name, lo, hi, n):
    q = f"substr(quest_id::text, 25)::bigint BETWEEN {lo} AND {hi}"
    u = f"substr(user_id::text, 25)::bigint BETWEEN {lo} AND {hi}"
    a = f"substr(aggregate_id::text, 25)::bigint BETWEEN {lo} AND {hi}"
    return f"""\\echo '=== {name}: users {lo}~{hi} (expect completed {n}, ledger rows {n}, xp {n * 10}, outbox {n} all PUBLISHED, users over 1 ledger row 0) ==='
SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'c1000002%' AND {q} AND status = 'COMPLETED') AS completed,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c1000001%' AND {u}) AS ledger_rows,
       (SELECT coalesce(sum(amount), 0) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c1000001%' AND {u}) AS xp,
       (SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'c1000002%' AND {a}) AS outbox,
       (SELECT count(*) FILTER (WHERE status <> 'PUBLISHED') FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'c1000002%' AND {a}) AS outbox_not_published,
       (SELECT count(*) FROM (SELECT user_id FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c1000001%' AND {u} GROUP BY user_id HAVING count(*) > 1) x) AS users_over_1;
"""


verify = group_sql("1) one broker down", 1, 100, 100) + group_sql("2) two brokers down", 201, 220, 20)

cleanup = """-- Kafka 3대 장애 데이터만 지운다
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'c100000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c100000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'c100000%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'c1000002%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'c100000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'c100000%';
COMMIT;
"""

files = {"01_seed.sql": seed, "03_verify.sql": verify, "99_cleanup.sql": cleanup,
         "p2.template.txt": "\n".join(ev(i) for i in P2) + "\n"}
for c in range(10):
    files[f"p1_{c + 1:02d}.template.txt"] = "\n".join(ev(i) for i in P1[c * 10:(c + 1) * 10]) + "\n"
for name, body in files.items():
    with open(os.path.join(here, name), "w", encoding="utf-8", newline="\n") as f:
        f.write(body)
print(f"유저 {len(users)}명 · 퀘스트 {len(users)}개. ① 10건 x 10묶음(유저 1~100) / ② 20건(유저 201~220)")
