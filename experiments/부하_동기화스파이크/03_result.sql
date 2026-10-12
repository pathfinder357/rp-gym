-- 변수: lo, hi (측정 유저 범위), t (measuredAt), n (주입 건수), send_start_ms, send_end_ms, drain_start_ms (컨슈머 해동 시각)
\pset pager off
CREATE TEMP TABLE r AS
SELECT updated_at,
       -- updated_at 은 JVM 기본 시간대(UTC)의 LocalDateTime 이다. 0번에서 확인한다
       extract(epoch FROM updated_at) * 1000 AS done_ms
FROM game_service.user_latest_snapshots
WHERE user_id BETWEEN :'lo'::uuid AND :'hi'::uuid
  AND measured_at = :'t'::timestamptz;

\echo '=== 0. 시계 점검 — 두 값이 수 분 이내여야 updated_at 이 UTC 다 (9시간 차이면 결과 무효) ==='
SELECT now() AT TIME ZONE 'UTC' AS db_utc_now, max(updated_at) AS last_done FROM r;

\echo '=== 1. 처리 건수와 소진 시간 ==='
SELECT count(*)                                                     AS processed,
       :n                                                           AS injected,
       round(((:send_end_ms - :send_start_ms) / 1000.0)::numeric, 2) AS send_sec,
       round(((max(done_ms) - :drain_start_ms) / 1000.0)::numeric, 2) AS drain_sec,
       round((count(*) / nullif((max(done_ms) - :drain_start_ms) / 1000.0, 0))::numeric, 1) AS per_sec,
       round(((max(done_ms) - :drain_start_ms) / nullif(count(*), 0) * 3)::numeric, 1) AS ms_per_event_per_thread
FROM r;

\echo '=== 2. 대기 시간 (해동 = 정각 → 스냅샷 저장), 초 ==='
SELECT round(percentile_cont(0.50) WITHIN GROUP (ORDER BY d)::numeric, 2) AS p50,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY d)::numeric, 2) AS p95,
       round(percentile_cont(0.99) WITHIN GROUP (ORDER BY d)::numeric, 2) AS p99,
       round(max(d)::numeric, 2)                                          AS max
FROM (SELECT (done_ms - :drain_start_ms) / 1000.0 AS d FROM r) x;

\echo '=== 3. 초당 처리량 (처음·마지막 1초 제외) ==='
WITH s AS (SELECT date_trunc('second', updated_at) sec, count(*) c FROM r GROUP BY 1),
     m AS (SELECT * FROM s WHERE sec > (SELECT min(sec) FROM s) AND sec < (SELECT max(sec) FROM s))
SELECT round(avg(c), 1) AS avg_per_sec, min(c) AS min_per_sec, max(c) AS max_per_sec, count(*) AS seconds FROM m;

\echo '=== 4. S2 전용 — 판정이 실제로 일어났는지 (S0 이면 전부 0) ==='
SELECT (SELECT count(*) FROM game_service.quests
         WHERE title = 'SPIKE-S2' AND last_applied_measured_at = :'t'::timestamptz)            AS quests_applied,
       (SELECT count(*) FROM game_service.party_quest_members m
         JOIN game_service.party_quests q USING (party_quest_id)
         WHERE q.title = 'SPIKE-S2' AND m.last_applied_measured_at = :'t'::timestamptz)        AS party_members_applied,
       (SELECT count(*) FROM game_service.party_quests
         WHERE title = 'SPIKE-S2' AND current_val <> (SELECT 0))                               AS party_quests_moved,
       (SELECT count(*) FROM game_service.party_quests q
         WHERE title = 'SPIKE-S2' AND current_val <> (SELECT coalesce(sum(contributed_val),0)
               FROM game_service.party_quest_members m WHERE m.party_quest_id = q.party_quest_id)) AS ledger_mismatch;
