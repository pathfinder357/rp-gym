-- ============================================================
-- 동시성 실험 데이터 준비 — 파티 100개 × 멤버 4명
--
-- 숫자를 이렇게 잡은 이유
--   기준값 1000, 이벤트 누적값 2000  → 멤버 한 사람의 기여가 정확히 1000
--   목표 4000                        → 네 명이 전부 반영돼야 도달한다
--
-- 한 명이라도 빠지면 완료되지 않고, 네 번째가 커밋되는 순간에만 완료가 가능하다.
-- 완료 선점에 가장 강한 경쟁이 걸리는 배치다.
--
-- 파티 담당자의 party_members 행은 만들지 않는다. 내 판정 경로가 그 테이블을 읽지 않기 때문이다.
-- parties 행만 필요한데, party_quests.party_id 가 거기로 물리 FK 를 걸고 있어서다.
-- ============================================================

BEGIN;

-- 1) 파티 100개
INSERT INTO game_service.parties
    (id, party_name, owner_id, status, visibility, metric, max_member, current_member,
     matching_deadline_at, ends_at)
SELECT
    ('11111111-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
    '실험파티 ' || i,
    ('33333333-0000-4000-8000-' || lpad(i::text, 9, '0') || '001')::uuid,
    'ACTIVE', 'PRIVATE', 'STEPS', 4, 4,
    now() - interval '1 hour',
    now() + interval '6 days'
FROM generate_series(1, 100) AS i;

-- 2) 유저별 최신 스냅샷 400개
--    측정 시각을 한 시간 전으로 둔다. 실험 이벤트의 측정 시각이 이보다 뒤여야
--    개인 퀘스트 쪽의 "더 새것일 때만 덮어쓴다" 판정을 통과한다.
INSERT INTO game_service.user_latest_snapshots
    (user_id, activity_date, measured_at, steps, active_minutes, active_calories)
SELECT
    ('33333333-0000-4000-8000-' || lpad(i::text, 9, '0') || lpad(j::text, 3, '0'))::uuid,
    (now() AT TIME ZONE 'Asia/Seoul')::date,
    now() - interval '1 hour',
    1000, 0, 0
FROM generate_series(1, 100) AS i, generate_series(1, 4) AS j;

-- 3) 파티 퀘스트 100개
--    생성 API 를 거치지 않고 직접 넣는다. 이 실험이 증명하려는 것은 판정 경로이고,
--    생성 경로는 단위 테스트가 이미 본다.
--    기한을 여섯 시간 뒤로 둔다. 자정 직전에 돌려도 이벤트가 만료로 걸러지지 않게 하려는 것이다.
INSERT INTO game_service.party_quests
    (party_quest_id, party_id, title, metric, target_val, current_val, status,
     reward_xp, started_at, expired_at)
SELECT
    ('22222222-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
    ('11111111-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
    '실험 파티 퀘스트 ' || i,
    'STEPS', 4000, 0, 'ACTIVE',
    30,
    now() - interval '1 hour',
    now() + interval '6 hours'
FROM generate_series(1, 100) AS i;

-- 4) 파티 퀘스트 멤버 400개
--    기준값을 미리 채워 둔다. 비워 두면 첫 이벤트가 기준만 정하고 기여를 잡지 않아서
--    완료까지 가려면 이벤트를 두 번 보내야 한다. 실험의 목적이 그것이 아니다.
INSERT INTO game_service.party_quest_members
    (party_quest_member_id, party_quest_id, user_id, baseline_val, contributed_val,
     last_applied_measured_at)
SELECT
    ('44444444-0000-4000-8000-' || lpad(i::text, 9, '0') || lpad(j::text, 3, '0'))::uuid,
    ('22222222-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
    ('33333333-0000-4000-8000-' || lpad(i::text, 9, '0') || lpad(j::text, 3, '0'))::uuid,
    1000, 0, NULL
FROM generate_series(1, 100) AS i, generate_series(1, 4) AS j;

COMMIT;

-- 준비 확인
SELECT
    (SELECT count(*) FROM game_service.parties)              AS 파티,
    (SELECT count(*) FROM game_service.party_quests)         AS 파티퀘스트,
    (SELECT count(*) FROM game_service.party_quest_members)  AS 멤버,
    (SELECT count(*) FROM game_service.user_latest_snapshots) AS 스냅샷;
-- 100 / 100 / 400 / 400 이어야 한다
