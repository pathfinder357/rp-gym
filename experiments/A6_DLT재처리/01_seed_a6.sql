-- A-6 시드 (gen_a6.py 가 만듦)
DO $$ BEGIN
  IF (now() AT TIME ZONE 'Asia/Seoul')::time > time '23:30' OR (now() AT TIME ZONE 'Asia/Seoul')::time < time '00:10' THEN
    RAISE EXCEPTION '자정 근처다. 어제/오늘 날짜가 갈리지 않게 00:10 ~ 23:30 사이에 하자';
  END IF;
END $$;
BEGIN;
-- F · D · Y: 개인 퀘스트 30개 (기준 1000 · 목표 500 · 보상 10). 오늘 이벤트(누적 2000)면 완료
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
SELECT ('a6000002-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('a6000001-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       gen_random_uuid(), 'A6 personal ' || i, 'STEPS', 500, 1000,
       now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, ((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul', 0
  FROM unnest(ARRAY[101,102,103,104,105,106,107,108,109,110,201,202,203,204,205,206,207,208,209,210,301,302,303,304,305,306,307,308,309,310]) AS i;
COMMIT;
SELECT count(*) AS a6_quests FROM game_service.quests WHERE quest_id::text LIKE 'a6000002%';
-- 30
