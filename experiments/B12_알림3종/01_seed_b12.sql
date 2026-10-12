-- B-12 시드 (gen_b12.py 가 만듦)
BEGIN;
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
VALUES ('b1200004-0000-4000-8000-000000000001', 'b1200003-0000-4000-8000-000000000001', gen_random_uuid(), 'B12 outbox quest', 'STEPS', 500, 1000,
        now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, ((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul', 0);
COMMIT;
SELECT count(*) AS b12_quests FROM game_service.quests WHERE quest_id::text LIKE 'b1200004%';
