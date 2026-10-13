"""알림(대조 · 랭킹 · 처리 지연) 새 알림 실험 — 데이터 생성기 (2026-10-12)

1) 대조 불일치(game-reconcile-mismatch): 유저 b1300001 은 완료 퀘스트 1개 · 원장 100 · 지갑 100 으로 정합한 상태로 시작.
   실행기가 랭킹을 재구축해 ZSET 도 맞춘 뒤, 지갑을 +50 으로 어긋낸다 -> ledger_wallet 1 · ranking 1 -> firing.
   다시 -50 으로 되돌리면 0 -> resolved
2) 대조 멈춤(game-reconcile-stale, 선택 · 약 31분): 실행기가 pause 파일을 만들면 대조가 건너뛰어져 last.success 가 멈춘다
3) 랭킹 반영 실패(game-ranking-update-failed): Redis 를 멈춘 채 유저 b1300003 의 퀘스트를 완료시키는 이벤트 1건
   -> 지급 뒤 랭킹 반영이 Redis 연결 실패 -> 실패 카운터 +1 -> firing (10분 창이 지나면 resolved)
4) 처리 지연 p95(game-pipeline-latency-high, 약 12분): 유저 b1300004 이벤트 12건(같은 파티션). 앱은 slow 파일이 있는 동안
   이 유저 이벤트를 70초씩 늦게 처리 -> 처리 지연 70 · 140 · 210 ...초가 계속 기록 -> p95 > 60초가 10분 지속 -> firing
5) Notification lag: Notification 서비스를 로컬에 띄우지 않아 이 실험에서 제외
식별자 앞자리 b13: 유저 b1300001 · b1300003 · b1300004 / 퀘스트 b1300002(완료) · b1300005(진행) / 원장 b1300006 / 이벤트 b1300009
"""
import os

here = os.path.dirname(os.path.abspath(__file__))
U = lambda pfx, n: f"{pfx}-0000-4000-8000-{n:012d}"
TODAY_END = "((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'"


def ev(eid, user, steps):
    return (f'{user}|{{"eventId":"{U("b1300009", eid)}","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"{user}",'
            f'"data":{{"activityDate":"__TODAY__","measuredAt":"__NOW__","cumulative":{{"steps":{steps},"activeMinutes":0,"activeCalories":0}}}}}}')


seed = f"""-- 알림(대조 · 랭킹 · 처리 지연) 시드 (gen.py 가 만듦)
BEGIN;
-- 1) 정합한 상태: 완료 퀘스트 1 · QUEST 원장 100 · 지갑 100
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
VALUES ('{U("b1300002", 1)}', '{U("b1300001", 1)}', gen_random_uuid(), 'B13 done quest', 'STEPS', 500, 1000,
        now() - interval '2 hour', now() - interval '1 hour', 2000, 'COMPLETED', 100, {TODAY_END}, 1);
INSERT INTO game_service.xp_ledgers (ledger_id, user_id, source_type, source_id, amount, occurred_at)
VALUES ('{U("b1300006", 1)}', '{U("b1300001", 1)}', 'QUEST', '{U("b1300002", 1)}', 100, now() - interval '1 hour');
INSERT INTO game_service.wallets (user_id, xp) VALUES ('{U("b1300001", 1)}', 100);
-- 3) 진행 중 퀘스트: 이벤트(누적 2000)가 오면 완료 · 보상 10
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
VALUES ('{U("b1300005", 1)}', '{U("b1300003", 1)}', gen_random_uuid(), 'B13 ranking quest', 'STEPS', 500, 1000,
        now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, {TODAY_END}, 0);
COMMIT;
SELECT (SELECT count(*) FROM game_service.quests WHERE user_id::text LIKE 'b130000%') AS b13_quests,
       (SELECT count(*) FROM game_service.wallets WHERE user_id::text LIKE 'b130000%') AS b13_wallets;
-- 2 / 1
"""
p3 = ev(300, U("b1300003", 1), 2000) + "\n"
p4 = "\n".join(ev(400 + i, U("b1300004", 1), 1000 + i * 100) for i in range(1, 13)) + "\n"
cleanup = """-- 알림(대조 · 랭킹 · 처리 지연) 데이터만 지운다
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b130000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'b130000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'b130000%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'b130000%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'b130000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'b130000%';
COMMIT;
"""
for name, body in [("01_seed.sql", seed), ("03_p3_ranking.template.txt", p3), ("04_p4_latency.template.txt", p4),
                   ("99_cleanup.sql", cleanup)]:
    with open(os.path.join(here, name), "w", encoding="utf-8", newline="\n") as f:
        f.write(body)
print("만든 파일: 01_seed.sql, 03_p3_ranking / 04_p4_latency .template.txt, 99_cleanup.sql")
