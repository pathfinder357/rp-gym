-- 낙관적 락 충돌 시드 (gen.py 가 만듦)
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
       now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, ((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul', 0
  FROM generate_series(1, 10) AS i;
COMMIT;
SELECT count(*) AS a8_quests FROM game_service.quests WHERE quest_id::text LIKE 'a8000002%';
-- 10
