"""낙관적 락 충돌 낙관적 락 충돌 실험 — 데이터 생성기 (2026-10-11)

개인 퀘스트 10개(기준 1000 · 목표 500 · 보상 10)에 오늘 이벤트(누적 2000) 1건씩.
앱은 실험 설정(conflict-user-prefix · conflict-times=5)으로 띄운다: 퀘스트를 읽은 직후 다른 커넥션이 version 을 올려
저장 때 낙관적 락 충돌이 퀘스트마다 5번 난다. 6번째 시도에서 성공해야 한다.
"그 밖의 예외"로 분류됐다면 3번 재시도 뒤 DLT 로 갔을 것 -> 일시 장애 분류(무한 재시도)의 실측 근거.
식별자 앞자리 a8: 유저 a8000001 / 퀘스트 a8000002 / 이벤트 a8000009
"""
import os

here = os.path.dirname(os.path.abspath(__file__))
U = lambda pfx, n: f"{pfx}-0000-4000-8000-{n:012d}"
TODAY_END = "((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'"
N = 10

seed = f"""-- 낙관적 락 충돌 시드 (gen.py 가 만듦)
DO $$ BEGIN
  IF (now() AT TIME ZONE 'Asia/Seoul')::time > time '23:30' OR (now() AT TIME ZONE 'Asia/Seoul')::time < time '00:10' THEN
    RAISE EXCEPTION '자정 근처다. 00:10 ~ 23:30 사이에 하자';
  END IF;
END $$;
BEGIN;
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
SELECT ('a8000002-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('a8000001-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       gen_random_uuid(), 'A8 personal ' || i, 'STEPS', 500, 1000,
       now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, {TODAY_END}, 0
  FROM generate_series(1, {N}) AS i;
COMMIT;
SELECT count(*) AS a8_quests FROM game_service.quests WHERE quest_id::text LIKE 'a8000002%';
-- {N}
"""

events = []
for i in range(1, N + 1):
    u = U("a8000001", i)
    events.append(f'{u}|{{"eventId":"{U("a8000009", i)}","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"{u}",'
                  f'"data":{{"activityDate":"__TODAY__","measuredAt":"__NOW__","cumulative":{{"steps":2000,"activeMinutes":0,"activeCalories":0}}}}}}')

verify = r"""-- 낙관적 락 충돌 검증
\echo '=== quests (expect COMPLETED 10, version = 5 conflicts + 1 own save = 6) ==='
SELECT status, count(*), min(version) AS min_version, max(version) AS max_version
  FROM game_service.quests WHERE quest_id::text LIKE 'a8000002%' GROUP BY status;
\echo '=== ledger (expect 10 rows / 100 xp, no duplicates) ==='
SELECT count(*) AS rows, coalesce(sum(amount), 0) AS xp, count(DISTINCT source_id) AS distinct_quests
  FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a8000001%';
\echo '=== wallet <> ledger (expect 0 rows) ==='
SELECT coalesce(w.user_id, l.user_id) AS user_id, w.xp AS wallet, l.total AS ledger
  FROM (SELECT user_id, sum(amount) AS total FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a8000001%' GROUP BY user_id) l
  FULL OUTER JOIN (SELECT * FROM game_service.wallets WHERE user_id::text LIKE 'a8000001%') w ON w.user_id = l.user_id
 WHERE coalesce(w.xp, 0) <> coalesce(l.total, 0);
\echo '=== QUEST_COMPLETED events (expect 10) ==='
SELECT count(*) FROM game_service.outbox_events WHERE event_type = 'QUEST_COMPLETED' AND aggregate_id::text LIKE 'a8000002%';
"""

cleanup = """-- 낙관적 락 충돌 데이터만 지운다
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a800000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a800000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'a800000%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'a8000002%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'a800000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'a800000%';
COMMIT;
"""
for name, body in [("01_seed.sql", seed), ("02_events.template.txt", "\n".join(events) + "\n"),
                   ("03_verify.sql", verify), ("99_cleanup.sql", cleanup)]:
    with open(os.path.join(here, name), "w", encoding="utf-8", newline="\n") as f:
        f.write(body)
print(f"개인 퀘스트 {N}개 · 이벤트 {N}건. 퀘스트마다 충돌 5번 -> 6번째 성공. 기대: 완료 {N} · 원장 {N}건 · DLT 0 · 충돌 로그 {N * 5}줄")
print("만든 파일: 01_seed.sql, 02_events.template.txt, 03_verify.sql, 99_cleanup.sql")
