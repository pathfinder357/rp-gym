-- B-15 배경 트래픽 유저(b1500001)의 흔적만 지운다 (퀘스트는 없고 최신 측정값만 쌓인다)
BEGIN;
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'b150000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'b150000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'b150000%';
COMMIT;
