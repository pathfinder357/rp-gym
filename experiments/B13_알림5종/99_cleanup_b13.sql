-- B-13 데이터만 지운다
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b130000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'b130000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'b130000%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'b130000%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'b130000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'b130000%';
COMMIT;
