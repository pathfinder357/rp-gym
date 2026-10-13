"""DB · Kafka 정지 의존 서비스 장애 주입 — 데이터 생성기 (2026-10-12)

질문: PostgreSQL · Kafka 가 잠깐 멈춰도 'XP 정확히 한 번' 이 지켜지는가 (DLT 0 · 유실 0 · 중복 0 · 복구 뒤 대조 0)
  P  PostgreSQL 60초 정지: 유저 1~150 의 이벤트를 4초마다 10건씩 흘리는 중에 DB 를 60초 멈춘다
     -> 일시 장애로 분류돼 무한 재시도(DLT 로 가면 안 됨) -> DB 가 돌아오면 밀린 것까지 전부 처리
  K  Kafka 90초 정지: 유저 201~300 의 이벤트 100건을 넣고 곧바로 Kafka 를 90초 멈춘다
     -> 처리 중이던 완료 이벤트는 Outbox 에 PENDING 으로 남고(발행 실패), 컨슈머는 재접속을 기다림
     -> Kafka 가 돌아오면 남은 유저 301~350 의 50건을 더 넣고 전부 처리 · Outbox 전부 발행
  유저마다 개인 퀘스트 1개(기준 1000 · 목표 500 · 보상 10), 이벤트 1건(누적 2000)으로 완료 -> 기대: 완료 = 원장 = 1건씩
식별자 앞자리 a16: 유저 a1600001 / 퀘스트 a1600002 / 이벤트 a1600009
"""
import os

here = os.path.dirname(os.path.abspath(__file__))
U = lambda pfx, n: f"{pfx}-0000-4000-8000-{n:012d}"
TODAY_END = "((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'"
P = list(range(1, 151))
K_PRE = list(range(201, 301))
K_POST = list(range(301, 351))


def ev(i):
    u = U("a1600001", i)
    return (f'{u}|{{"eventId":"{U("a1600009", i)}","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"{u}",'
            f'"data":{{"activityDate":"__TODAY__","measuredAt":"__NOW__","cumulative":{{"steps":2000,"activeMinutes":0,"activeCalories":0}}}}}}')


users = P + K_PRE + K_POST
seed = f"""-- DB · Kafka 정지 시드 (gen.py 가 만듦)
DO $$ BEGIN
  IF (now() AT TIME ZONE 'Asia/Seoul')::time > time '23:30' THEN RAISE EXCEPTION '자정 근처다. 23:30 전에 하자'; END IF;
END $$;
BEGIN;
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
SELECT ('a1600002-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('a1600001-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       gen_random_uuid(), 'B16 personal ' || i, 'STEPS', 500, 1000,
       now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, {TODAY_END}, 0
  FROM unnest(ARRAY[{",".join(map(str, users))}]) AS i;
COMMIT;
SELECT count(*) AS b16_quests FROM game_service.quests WHERE quest_id::text LIKE 'a1600002%';
-- {len(users)}
"""


def group_sql(name, lo, hi, n):
    q = f"substr(quest_id::text, 25)::bigint BETWEEN {lo} AND {hi}"
    u = f"substr(user_id::text, 25)::bigint BETWEEN {lo} AND {hi}"
    return f"""\\echo '=== {name}: users {lo}~{hi} (expect completed {n}, ledger rows {n}, distinct quests {n}, xp {n * 10}, outbox {n} all PUBLISHED) ==='
SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a1600002%' AND {q} AND status = 'COMPLETED') AS completed,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' AND {u}) AS ledger_rows,
       (SELECT count(DISTINCT source_id) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' AND {u}) AS distinct_quests,
       (SELECT coalesce(sum(amount), 0) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' AND {u}) AS xp,
       (SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a1600002%' AND substr(aggregate_id::text, 25)::bigint BETWEEN {lo} AND {hi}) AS outbox,
       (SELECT count(*) FILTER (WHERE status <> 'PUBLISHED') FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a1600002%' AND substr(aggregate_id::text, 25)::bigint BETWEEN {lo} AND {hi}) AS outbox_not_published;
"""


verify = (group_sql("P (PostgreSQL 60s)", 1, 150, 150) + group_sql("K (Kafka 90s)", 201, 350, 150) + r"""
\echo '=== all a16 users: wallet <> ledger (expect 0 rows) ==='
SELECT coalesce(w.user_id, l.user_id) AS user_id, w.xp AS wallet, l.total AS ledger
  FROM (SELECT user_id, sum(amount) AS total FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' GROUP BY user_id) l
  FULL OUTER JOIN (SELECT * FROM game_service.wallets WHERE user_id::text LIKE 'a1600001%') w ON w.user_id = l.user_id
 WHERE coalesce(w.xp, 0) <> coalesce(l.total, 0);
\echo '=== users with more than one ledger row (expect 0 rows = no double grant) ==='
SELECT user_id, count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' GROUP BY user_id HAVING count(*) > 1;
""")

cleanup = """-- DB · Kafka 정지 데이터만 지운다
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a160000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a160000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'a160000%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'a1600002%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'a160000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'a160000%';
COMMIT;
"""

files = {"01_seed.sql": seed, "03_verify.sql": verify, "99_cleanup.sql": cleanup,
         "k_pre.template.txt": "\n".join(ev(i) for i in K_PRE) + "\n",
         "k_post.template.txt": "\n".join(ev(i) for i in K_POST) + "\n"}
for c in range(15):
    files[f"p_{c + 1:02d}.template.txt"] = "\n".join(ev(i) for i in P[c * 10:(c + 1) * 10]) + "\n"
for name, body in files.items():
    with open(os.path.join(here, name), "w", encoding="utf-8", newline="\n") as f:
        f.write(body)
print(f"유저 {len(users)}명 · 퀘스트 {len(users)}개. P: 10건 x 15묶음(유저 1~150) / K: 앞 100건(201~300) + 뒤 50건(301~350)")
print("만든 파일: 01_seed.sql, 03_verify.sql, 99_cleanup.sql, p_01~15 / k_pre / k_post .template.txt")
