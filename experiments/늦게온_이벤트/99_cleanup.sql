-- 늦게 온 이벤트 데이터만 지운다 (a5 로 시작하는 식별자)
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a500000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a500000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'a500000%';
DELETE FROM game_service.quest_suggestions WHERE suggestion_id::text LIKE 'a5000008%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'a5000002%';
DELETE FROM game_service.party_quest_members WHERE party_quest_id::text LIKE 'a5000004%';
DELETE FROM game_service.party_quests WHERE party_quest_id::text LIKE 'a5000004%';
DELETE FROM game_service.parties WHERE id::text LIKE 'a5000003%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'a500000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'a500000%';
COMMIT;
