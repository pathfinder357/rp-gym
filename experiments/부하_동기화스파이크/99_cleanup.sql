-- 부하 실험 데이터만 지운다. 실험 유저는 전부 66666666-0000-4000- 으로 시작한다.
-- 서버에 실제 사용 데이터가 있으므로 다른 조건으로 지우지 않는다.
\set lo '66666666-0000-4000-0000-000000000000'
\set hi '66666666-0000-4000-ffff-ffffffffffff'
BEGIN;
DELETE FROM game_service.outbox_events         WHERE partition_key LIKE '66666666-0000-4000-%';
DELETE FROM game_service.xp_ledgers            WHERE user_id BETWEEN :'lo' AND :'hi';
DELETE FROM game_service.wallets               WHERE user_id BETWEEN :'lo' AND :'hi';
DELETE FROM game_service.characters            WHERE user_id BETWEEN :'lo' AND :'hi';
DELETE FROM game_service.quest_suggestions     WHERE user_id BETWEEN :'lo' AND :'hi';
DELETE FROM game_service.quests                WHERE user_id BETWEEN :'lo' AND :'hi';
DELETE FROM game_service.party_quest_members   WHERE user_id BETWEEN :'lo' AND :'hi';
DELETE FROM game_service.party_quests          WHERE title LIKE 'SPIKE-%';
DELETE FROM game_service.parties               WHERE party_name LIKE 'SPIKE-%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id BETWEEN :'lo' AND :'hi';
COMMIT;
