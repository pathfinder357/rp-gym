-- A-5 시드 (gen_a5.py 가 만듦)
DO $$ BEGIN
  IF (now() AT TIME ZONE 'Asia/Seoul')::time > time '23:30' OR (now() AT TIME ZONE 'Asia/Seoul')::time < time '00:10' THEN
    RAISE EXCEPTION '자정 근처다. 어제/오늘 날짜가 갈리지 않게 00:10 ~ 23:30 사이에 하자';
  END IF;
END $$;
BEGIN;
-- P: 개인 퀘스트 50개 (1~25 = P-A, 26~50 = P-B)
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
SELECT ('a5000002-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('a5000001-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       gen_random_uuid(), 'A5 개인 ' || i, 'STEPS', 2000, 3000,
       now() - interval '1 minute', NULL, NULL, 'ACTIVE', 50, ((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul', 0
  FROM generate_series(1, 50) AS i;
-- Q: 파티 25개 × 4명
INSERT INTO game_service.parties (id, party_name, owner_id, status, visibility, metric, max_member, current_member,
    matching_deadline_at, ends_at)
SELECT ('a5000003-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid, 'A5 파티 ' || i,
       ('a5000005-0000-4000-8000-' || lpad((i * 10 + 1)::text, 12, '0'))::uuid,
       'ACTIVE', 'PRIVATE', 'STEPS', 4, 4, now() - interval '1 hour', now() + interval '6 days'
  FROM generate_series(1, 25) AS i;
INSERT INTO game_service.party_quests (party_quest_id, party_id, title, metric, target_val, current_val, status,
    reward_xp, started_at, expired_at)
SELECT ('a5000004-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('a5000003-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       'A5 파티 퀘스트 ' || i, 'STEPS', 4000, 0, 'ACTIVE', 30, now() - interval '1 minute', ((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'
  FROM generate_series(1, 25) AS i;
INSERT INTO game_service.party_quest_members (party_quest_member_id, party_quest_id, user_id, baseline_val,
    contributed_val, last_applied_measured_at)
SELECT ('a5000006-0000-4000-8000-' || lpad((i * 10 + j)::text, 12, '0'))::uuid,
       ('a5000004-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('a5000005-0000-4000-8000-' || lpad((i * 10 + j)::text, 12, '0'))::uuid, 1000, 0, NULL
  FROM generate_series(1, 25) AS i, generate_series(1, 4) AS j;
COMMIT;
SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a5000002%') AS personal_quests,
       (SELECT count(*) FROM game_service.party_quests WHERE party_quest_id::text LIKE 'a5000004%') AS party_quests,
       (SELECT count(*) FROM game_service.party_quest_members WHERE party_quest_id::text LIKE 'a5000004%') AS party_members;
-- 50 / 25 / 100
