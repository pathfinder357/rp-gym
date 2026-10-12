-- ============================================================
-- Outbox publish latency -- how long a row waits before it reaches Kafka
--
-- This is NOT the throughput test. Rows are injected one at a time with a
-- gap, so the relay is never saturated. What we measure is the wait itself:
--
--     latency = published_at - created_at
--
-- The relay polls with @Scheduled(fixedDelay = poll-interval). A row that
-- lands right after a round just missed it and waits a full cycle.
-- So the expected shape is: avg ~ cycle/2, max ~ cycle.
--
-- Why it matters: this event becomes the Slack card the user sees.
--
-- Prerequisites -- same as the throughput test. postgres + kafka +
-- game-service must be running. This script starts nothing.
--
-- Run
--   Get-Content .\실험_outbox_지연.sql | docker exec -i rp-gym-postgres-game psql -U postgres -d game_db
--
-- Takes about 60 seconds (40 rows x 1.3s + drain wait).
-- ============================================================

\pset pager off

\echo ''
\echo '=== 0. timezone sanity check ==='
-- created_at is TIMESTAMP (no zone), published_at is TIMESTAMPTZ.
-- If the DB session zone is not UTC the subtraction below is skewed.
-- Expect "UTC". If it is not, latency numbers will be off by whole hours.
SHOW timezone;
SELECT now() AS db_now_tz, now()::timestamp AS db_now_naive;

\echo ''
\echo '=== 1. current relay settings (for the record) ==='
\echo '    poll-interval / poll-size come from application.yml -- write them down by hand'

\echo ''
\echo '=== 2. clean previous latency rows ==='
DELETE FROM game_service.outbox_events WHERE partition_key = 'LATENCY-TEST';

\echo ''
\echo '=== 3. inject 40 rows, one every 1.3s (relay stays unsaturated) ==='
\echo '    this takes about 55 seconds and prints INSERT 0 1 each time'

INSERT INTO game_service.outbox_events
    (outbox_id, aggregate_type, aggregate_id, event_type, event_id,
     partition_key, payload, status, retry_count, created_at, published_at)
VALUES
    (gen_random_uuid(), 'QUEST', gen_random_uuid(), 'QUEST_COMPLETED', gen_random_uuid(),
     'LATENCY-TEST',
     '{"eventType":"QUEST_COMPLETED","note":"latency probe"}'::jsonb,
     'PENDING', 0, now(), NULL);
\watch i=1.3 c=40

\echo ''
\echo '=== 4. wait for the tail to drain ==='
SELECT pg_sleep(5);

\echo ''
\echo '=== 5. latency distribution ==='
SELECT count(*)                                               AS rows,
       count(*) FILTER (WHERE published_at IS NULL)           AS not_published_yet,
       round(min(ms))                                         AS min_ms,
       round(avg(ms))                                         AS avg_ms,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY ms)) AS p50_ms,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY ms))AS p95_ms,
       round(max(ms))                                         AS max_ms
  FROM (
    SELECT extract(epoch FROM (published_at - (created_at AT TIME ZONE 'UTC'))) * 1000 AS ms,
           published_at
      FROM game_service.outbox_events
     WHERE partition_key = 'LATENCY-TEST'
  ) t;

\echo ''
\echo '=== 6. raw values (to eyeball the shape -- should spread 0..cycle) ==='
SELECT row_number() OVER (ORDER BY created_at) AS n,
       round((extract(epoch FROM (published_at - (created_at AT TIME ZONE 'UTC'))) * 1000)::numeric) AS latency_ms
  FROM game_service.outbox_events
 WHERE partition_key = 'LATENCY-TEST'
 ORDER BY created_at;

-- ============================================================
-- Next: change the setting and run this file again.
--
--   game-service/src/main/resources/application.yml
--     rpgym.outbox.poll-interval: 1000   ->   200
--
--   restart game-service, then re-run this script.
--
-- Record both runs:
--
--                     poll-interval 1000    poll-interval 200
--     avg_ms          ______                ______
--     p95_ms          ______                ______
--     max_ms          ______                ______
--
-- Also note the cost side: a shorter interval means the relay queries the
-- table more often while it is idle. At 0.23 events/sec real traffic that is
-- mostly empty polls. That trade-off belongs in the writeup.
-- ============================================================

-- cleanup (run after writing the numbers down)
-- DELETE FROM game_service.outbox_events WHERE partition_key = 'LATENCY-TEST';
