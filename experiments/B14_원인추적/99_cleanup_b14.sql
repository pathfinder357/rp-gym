-- B-14 데이터만 지운다
BEGIN;
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'b140000%';
COMMIT;
