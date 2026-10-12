-- B-13 시드 (gen_b13.py 가 만듦)
BEGIN;
-- 1) 정합한 상태: 완료 퀘스트 1 · QUEST 원장 100 · 지갑 100
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
VALUES ('b1300002-0000-4000-8000-000000000001', 'b1300001-0000-4000-8000-000000000001', gen_random_uuid(), 'B13 done quest', 'STEPS', 500, 1000,
        now() - interval '2 hour', now() - interval '1 hour', 2000, 'COMPLETED', 100, ((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul', 1);
INSERT INTO game_service.xp_ledgers (ledger_id, user_id, source_type, source_id, amount, occurred_at)
VALUES ('b1300006-0000-4000-8000-000000000001', 'b1300001-0000-4000-8000-000000000001', 'QUEST', 'b1300002-0000-4000-8000-000000000001', 100, now() - interval '1 hour');
INSERT INTO game_service.wallets (user_id, xp) VALUES ('b1300001-0000-4000-8000-000000000001', 100);
-- 3) 진행 중 퀘스트: 이벤트(누적 2000)가 오면 완료 · 보상 10
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
VALUES ('b1300005-0000-4000-8000-000000000001', 'b1300003-0000-4000-8000-000000000001', gen_random_uuid(), 'B13 ranking quest', 'STEPS', 500, 1000,
        now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, ((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul', 0);
COMMIT;
SELECT (SELECT count(*) FROM game_service.quests WHERE user_id::text LIKE 'b130000%') AS b13_quests,
       (SELECT count(*) FROM game_service.wallets WHERE user_id::text LIKE 'b130000%') AS b13_wallets;
-- 2 / 1
