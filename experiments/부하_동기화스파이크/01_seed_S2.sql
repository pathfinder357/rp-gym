-- S2 무거운 끝: n 명 전원이 진행 중인 개인 퀘스트 1개 + 4인 파티 퀘스트 1개를 갖는다.
-- 이번 이벤트로 둘 다 진행만 하고 완료는 안 되게 목표를 크게 잡는다.
--   개인:  baseline 1000, 이벤트 2000  → 달성 1000 / 목표 100000
--   파티:  멤버마다 기여 1000          → 합계 4000 / 목표 1000000
-- 그래서 이 시나리오가 재는 것은 "판정 + 진행 갱신 + 파티 공유 행 경합" 이고, XP 지급·outbox 는 타지 않는다.
-- 컬럼 구성은 2026-09-27 lost update 실험 시드가 이 서버에서 실제로 통과한 것과 같다.
-- 변수: n (4의 배수), prefix (userId 앞 24자리)
\set ON_ERROR_STOP on
BEGIN;

-- 스냅샷: S0 과 같다. 30분 전 값이 있어서 정각 이벤트가 UPDATE 경로를 탄다
INSERT INTO game_service.user_latest_snapshots
  (user_id, activity_date, measured_at, steps, active_minutes, active_calories, created_at, updated_at)
SELECT (:'prefix' || lpad(g::text, 12, '0'))::uuid,
       ((now() - interval '30 minutes') AT TIME ZONE 'Asia/Seoul')::date,
       now() - interval '30 minutes', 1000, 10, 50,
       now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'
FROM generate_series(1, :n) g;

-- 개인 퀘스트
INSERT INTO game_service.quests
  (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
   baseline_measured_at, status, reward_xp, expired_at)
SELECT ('66666666-0000-4000-c000-' || lpad(g::text, 12, '0'))::uuid,
       (:'prefix' || lpad(g::text, 12, '0'))::uuid,
       ('66666666-0000-4000-d000-' || lpad(g::text, 12, '0'))::uuid,
       'SPIKE-S2', 'STEPS', 100000, 1000,
       now() - interval '1 hour', 'ACTIVE', 5, now() + interval '6 hours'
FROM generate_series(1, :n) g;

-- 파티 n/4 개. 파티장은 각 파티의 첫 번째 멤버
INSERT INTO game_service.parties
  (id, party_name, owner_id, status, visibility, metric, max_member, current_member, matching_deadline_at, ends_at)
SELECT ('66666666-0000-4000-a000-' || lpad(p::text, 12, '0'))::uuid,
       'SPIKE-' || p,
       (:'prefix' || lpad(((p - 1) * 4 + 1)::text, 12, '0'))::uuid,
       'ACTIVE', 'PRIVATE', 'STEPS', 4, 4, now(), now() + interval '7 days'
FROM generate_series(1, :n / 4) p;

INSERT INTO game_service.party_quests
  (party_quest_id, party_id, title, metric, target_val, current_val, status, reward_xp, started_at, expired_at)
SELECT ('66666666-0000-4000-b000-' || lpad(p::text, 12, '0'))::uuid,
       ('66666666-0000-4000-a000-' || lpad(p::text, 12, '0'))::uuid,
       'SPIKE-S2', 'STEPS', 1000000, 0, 'ACTIVE', 30,
       now() - interval '1 hour', now() + interval '6 hours'
FROM generate_series(1, :n / 4) p;

INSERT INTO game_service.party_quest_members
  (party_quest_member_id, party_quest_id, user_id, baseline_val, contributed_val)
SELECT gen_random_uuid(),
       ('66666666-0000-4000-b000-' || lpad(((g - 1) / 4 + 1)::text, 12, '0'))::uuid,
       (:'prefix' || lpad(g::text, 12, '0'))::uuid,
       1000, 0
FROM generate_series(1, :n) g;

COMMIT;

SELECT (SELECT count(*) FROM game_service.quests WHERE title = 'SPIKE-S2')       AS quests,
       (SELECT count(*) FROM game_service.party_quests WHERE title = 'SPIKE-S2') AS party_quests,
       (SELECT count(*) FROM game_service.party_quest_members
         WHERE user_id BETWEEN (:'prefix' || '000000000000')::uuid AND (:'prefix' || 'ffffffffffff')::uuid) AS members;
