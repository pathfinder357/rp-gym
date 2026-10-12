-- Load test step 1 : inject N PENDING rows into outbox_events.
-- ASCII only (PowerShell pipe safe).
--
-- We inject directly into the outbox table instead of pushing Kafka events,
-- because the target under test is the relay (poll -> publish), not the consumer.
--
-- Change 5000 below to adjust the load size.

\pset pager off

-- clean previous load rows first
DELETE FROM game_service.outbox_events
 WHERE outbox_id::text LIKE 'aaaaaaaa-0000-4000-8000-%';

INSERT INTO game_service.outbox_events
    (outbox_id, aggregate_type, aggregate_id, event_type, event_id,
     partition_key, payload, status, retry_count, created_at, published_at)
SELECT
    ('aaaaaaaa-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
    'QUEST',
    ('bbbbbbbb-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
    'QUEST_COMPLETED',
    ('cccccccc-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
    -- partition key = userId. spread across partitions like real traffic.
    ('dddddddd-0000-4000-8000-' || lpad(i::text, 12, '0')),
    json_build_object(
        'eventId',   'cccccccc-0000-4000-8000-' || lpad(i::text, 12, '0'),
        'eventType', 'QUEST_COMPLETED',
        'userId',    'dddddddd-0000-4000-8000-' || lpad(i::text, 12, '0'),
        'occurredAt','2026-09-23T00:00:00Z',
        'data', json_build_object(
            'questId',  'bbbbbbbb-0000-4000-8000-' || lpad(i::text, 12, '0'),
            'title',    'load test quest',
            'metric',   'STEPS',
            'targetValue', 1500,
            'rewardXp', 5)
    )::jsonb,
    'PENDING',
    0,
    now(),
    NULL
FROM generate_series(1, 5000) AS i;

SELECT status, count(*) FROM game_service.outbox_events
 WHERE outbox_id::text LIKE 'aaaaaaaa-0000-4000-8000-%'
 GROUP BY status;
