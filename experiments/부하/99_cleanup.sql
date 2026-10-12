-- Load test cleanup : remove injected rows.
DELETE FROM game_service.outbox_events
 WHERE outbox_id::text LIKE 'aaaaaaaa-0000-4000-8000-%';

VACUUM ANALYZE game_service.outbox_events;

SELECT status, count(*) FROM game_service.outbox_events GROUP BY status;
