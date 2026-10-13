-- DLT 재처리 데이터만 지운다 (a6 로 시작하는 식별자)
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a600000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a600000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'a600000%';
DELETE FROM game_service.quest_suggestions WHERE suggestion_id::text LIKE 'a6000008%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'a6000002%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'a600000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'a600000%';
COMMIT;
