-- ============================================================
-- Load test -- Outbox relay publish throughput  (single script)
--
-- What it measures
--   How many events/sec the relay can push out, and how the PENDING
--   backlog drains. Rows are injected straight into outbox_events
--   because the target under test is the relay (poll -> publish),
--   not the consumer or the quest judgement path.
--
-- Why this is the ceiling (see the code)
--   OutboxRelay.relayOnce()      for (e : pending) relay(e);        // up to poll-size
--   OutboxKafkaPublisher.publish kafkaTemplate.send(r).get(5s);     // one at a time, waits
--   OutboxPublishScheduler       @Scheduled(fixedDelay = 1000)      // 1s gap between rounds
--
--   throughput = poll-size / (round duration + poll-interval)
--              = 100 / (100 x round-trip + 1s)
--
--   Two levers: poll-size, and the synchronous wait. Measure first, then fix.
--
-- Prerequisites -- this script does NOT start anything. Bring these up first.
--
--   1) infra (postgres x3, kafka, redis)
--      docker compose -f docker-compose-develop.yml up -d
--      docker compose -f docker-compose-develop.yml ps      -- postgres must be healthy
--
--   2) discovery-server  (skip it and the logs fill with eureka stack traces)
--      .\gradlew.bat :discovery-server:bootRun --console=plain
--
--   3) game-service      (IntelliJ Run is easier; gradlew also works)
--      .\gradlew.bat :game-service:bootRun --console=plain
--
--   Kafka is required -- it is what the relay publishes to. Without it every
--   publish fails, retries forever, PENDING never drains and this script hits
--   the 300s cap. Redis is NOT required: injecting outbox rows directly does
--   not go through XP granting, so the ranking listener never fires.
--   No consumer is needed on game.events.
--
--   Sanity check: right after seeding, the game-service log should print
--   "Outbox 이벤트를 발행했다. count=100" about once per second.
--
-- Run
--   Get-Content .\실험_outbox_부하.sql | docker exec -i rp-gym-postgres-game psql -U postgres -d game_db
--
-- Takes roughly (seed / throughput) seconds. Safety cap is 300s.
-- ============================================================

\pset pager off

\echo ''
\echo '=== 0. environment ==='
SELECT version();
SELECT name, setting FROM pg_settings WHERE name IN ('shared_buffers','max_connections');

\echo ''
\echo '=== 1. clean previous load rows ==='
DELETE FROM game_service.outbox_events
 WHERE outbox_id::text LIKE 'aaaaaaaa-0000-4000-8000-%';

\echo ''
\echo '=== 2. seed 5,000 PENDING rows ==='
-- change 5000 to adjust the load size
INSERT INTO game_service.outbox_events
    (outbox_id, aggregate_type, aggregate_id, event_type, event_id,
     partition_key, payload, status, retry_count, created_at, published_at)
SELECT
    ('aaaaaaaa-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
    'QUEST',
    ('bbbbbbbb-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
    'QUEST_COMPLETED',
    ('cccccccc-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
    -- partition key = userId, spread across partitions like real traffic
    ('dddddddd-0000-4000-8000-' || lpad(i::text, 12, '0')),
    json_build_object(
        'eventId',    'cccccccc-0000-4000-8000-' || lpad(i::text, 12, '0'),
        'eventType',  'QUEST_COMPLETED',
        'userId',     'dddddddd-0000-4000-8000-' || lpad(i::text, 12, '0'),
        'occurredAt', '2026-09-23T00:00:00Z',
        'data', json_build_object(
            'questId',     'bbbbbbbb-0000-4000-8000-' || lpad(i::text, 12, '0'),
            'title',       'load test quest',
            'metric',      'STEPS',
            'targetValue', 1500,
            'rewardXp',    5)
    )::jsonb,
    'PENDING', 0, now(), NULL
FROM generate_series(1, 5000) AS i;

\echo ''
\echo '=== 3. sampling every second until drained (cap 300s) ==='
\echo '    this block prints nothing while it runs. wait for it.'

CREATE TEMP TABLE IF NOT EXISTS load_samples (
    sample_no int, at timestamptz, pending bigint, published bigint);
TRUNCATE load_samples;

DO $$
DECLARE
    i           int    := 0;
    v_pending   bigint;
    v_published bigint;
    zero_streak int    := 0;
BEGIN
    LOOP
        i := i + 1;

        -- READ COMMITTED gives each statement a fresh snapshot,
        -- so the relay's commits become visible inside this loop.
        SELECT count(*) FILTER (WHERE status = 'PENDING'),
               count(*) FILTER (WHERE status = 'PUBLISHED')
          INTO v_pending, v_published
          FROM game_service.outbox_events
         WHERE outbox_id::text LIKE 'aaaaaaaa-0000-4000-8000-%';

        INSERT INTO load_samples VALUES (i, clock_timestamp(), v_pending, v_published);

        IF v_pending = 0 THEN
            zero_streak := zero_streak + 1;
            EXIT WHEN zero_streak >= 2;
        ELSE
            zero_streak := 0;
        END IF;

        EXIT WHEN i >= 300;
        PERFORM pg_sleep(1);
    END LOOP;
END $$;

\echo ''
\echo '=== 4. drain curve  (delta = events published in that second) ==='
SELECT sample_no,
       round(extract(epoch FROM (at - min(at) OVER ()))::numeric, 1) AS elapsed_sec,
       pending,
       published,
       published - lag(published, 1, 0::bigint) OVER (ORDER BY sample_no) AS delta
  FROM load_samples
 ORDER BY sample_no;

\echo ''
\echo '=== 5. summary ==='
SELECT max(published)                                                        AS total_published,
       round(extract(epoch FROM (max(at) - min(at)))::numeric, 1)            AS elapsed_sec,
       round((max(published) /
              NULLIF(extract(epoch FROM (max(at) - min(at))), 0))::numeric, 1) AS avg_events_per_sec,
       max(pending)                                                          AS peak_backlog
  FROM load_samples;

\echo ''
\echo '=== 6. still pending? (should be 0; if not, the cap was hit) ==='
SELECT count(*) AS still_pending
  FROM game_service.outbox_events
 WHERE outbox_id::text LIKE 'aaaaaaaa-0000-4000-8000-%' AND status = 'PENDING';

-- ============================================================
-- cleanup -- run after writing the numbers down
--
-- DELETE FROM game_service.outbox_events
--  WHERE outbox_id::text LIKE 'aaaaaaaa-0000-4000-8000-%';
-- VACUUM ANALYZE game_service.outbox_events;
-- ============================================================
