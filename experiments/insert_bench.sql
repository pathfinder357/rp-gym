-- Outbox index write-cost benchmark. ASCII only (PowerShell pipe safe).
-- Alternates composite <-> partial twice to rule out cache/order effects.
-- Each INSERT runs inside BEGIN/ROLLBACK so nothing is persisted.
-- EXPLAIN ANALYZE prints "Execution Time" so \timing is not needed.

\pset pager off

\echo '=== ROUND 1 : COMPOSITE (status, created_at) ==='
BEGIN;
EXPLAIN (ANALYZE, TIMING ON, COSTS OFF)
INSERT INTO game_service.outbox_events
    (outbox_id, aggregate_type, aggregate_id, event_type, event_id,
     partition_key, payload, status, retry_count, created_at, published_at)
SELECT ('66666666-0000-4000-8000-' || lpad((610000 + i)::text, 12, '0'))::uuid,
       'QUEST',
       ('88888888-0000-4000-8000-' || lpad((610000 + i)::text, 12, '0'))::uuid,
       'QUEST_COMPLETED',
       ('77777777-0000-4000-8000-' || lpad((610000 + i)::text, 12, '0'))::uuid,
       ('99999999-0000-4000-8000-' || lpad((610000 + i)::text, 12, '0')),
       '{"eventId":"seed"}'::jsonb, 'PENDING', 0, now(), NULL
FROM generate_series(1, 1000) AS i;
ROLLBACK;

\echo '=== switch to PARTIAL ==='
CREATE INDEX idx_outbox_events_pending
    ON game_service.outbox_events (created_at) WHERE status = 'PENDING';
DROP INDEX game_service.idx_outbox_events_status;

\echo '=== ROUND 2 : PARTIAL (created_at) WHERE status = PENDING ==='
BEGIN;
EXPLAIN (ANALYZE, TIMING ON, COSTS OFF)
INSERT INTO game_service.outbox_events
    (outbox_id, aggregate_type, aggregate_id, event_type, event_id,
     partition_key, payload, status, retry_count, created_at, published_at)
SELECT ('66666666-0000-4000-8000-' || lpad((620000 + i)::text, 12, '0'))::uuid,
       'QUEST',
       ('88888888-0000-4000-8000-' || lpad((620000 + i)::text, 12, '0'))::uuid,
       'QUEST_COMPLETED',
       ('77777777-0000-4000-8000-' || lpad((620000 + i)::text, 12, '0'))::uuid,
       ('99999999-0000-4000-8000-' || lpad((620000 + i)::text, 12, '0')),
       '{"eventId":"seed"}'::jsonb, 'PENDING', 0, now(), NULL
FROM generate_series(1, 1000) AS i;
ROLLBACK;

\echo '=== switch back to COMPOSITE ==='
CREATE INDEX idx_outbox_events_status
    ON game_service.outbox_events (status, created_at);
DROP INDEX game_service.idx_outbox_events_pending;

\echo '=== ROUND 3 : COMPOSITE again ==='
BEGIN;
EXPLAIN (ANALYZE, TIMING ON, COSTS OFF)
INSERT INTO game_service.outbox_events
    (outbox_id, aggregate_type, aggregate_id, event_type, event_id,
     partition_key, payload, status, retry_count, created_at, published_at)
SELECT ('66666666-0000-4000-8000-' || lpad((630000 + i)::text, 12, '0'))::uuid,
       'QUEST',
       ('88888888-0000-4000-8000-' || lpad((630000 + i)::text, 12, '0'))::uuid,
       'QUEST_COMPLETED',
       ('77777777-0000-4000-8000-' || lpad((630000 + i)::text, 12, '0'))::uuid,
       ('99999999-0000-4000-8000-' || lpad((630000 + i)::text, 12, '0')),
       '{"eventId":"seed"}'::jsonb, 'PENDING', 0, now(), NULL
FROM generate_series(1, 1000) AS i;
ROLLBACK;

\echo '=== switch to PARTIAL again ==='
CREATE INDEX idx_outbox_events_pending
    ON game_service.outbox_events (created_at) WHERE status = 'PENDING';
DROP INDEX game_service.idx_outbox_events_status;

\echo '=== ROUND 4 : PARTIAL again ==='
BEGIN;
EXPLAIN (ANALYZE, TIMING ON, COSTS OFF)
INSERT INTO game_service.outbox_events
    (outbox_id, aggregate_type, aggregate_id, event_type, event_id,
     partition_key, payload, status, retry_count, created_at, published_at)
SELECT ('66666666-0000-4000-8000-' || lpad((640000 + i)::text, 12, '0'))::uuid,
       'QUEST',
       ('88888888-0000-4000-8000-' || lpad((640000 + i)::text, 12, '0'))::uuid,
       'QUEST_COMPLETED',
       ('77777777-0000-4000-8000-' || lpad((640000 + i)::text, 12, '0'))::uuid,
       ('99999999-0000-4000-8000-' || lpad((640000 + i)::text, 12, '0')),
       '{"eventId":"seed"}'::jsonb, 'PENDING', 0, now(), NULL
FROM generate_series(1, 1000) AS i;
ROLLBACK;

\echo '=== final index state ==='
SELECT indexname,
       pg_size_pretty(pg_relation_size(
           (quote_ident(schemaname) || '.' || quote_ident(indexname))::regclass)) AS size
  FROM pg_indexes
 WHERE schemaname = 'game_service' AND tablename = 'outbox_events'
 ORDER BY indexname;
