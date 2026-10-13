-- Kafka 3대 장애 데이터만 지운다
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'c100000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c100000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'c100000%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'c1000002%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'c100000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'c100000%';
COMMIT;
