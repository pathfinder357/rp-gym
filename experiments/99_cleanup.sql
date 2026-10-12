-- 실험 데이터만 지운다. 식별자 앞자리로 구분한다.
-- 순서가 중요하다. 참조하는 쪽부터 지운다.
BEGIN;

-- (A-3 · A-4) 개인 퀘스트 · 그 원장 · 그 완료 이벤트. 없으면 0행
DELETE FROM game_service.outbox_events
 WHERE aggregate_id::text LIKE '66666666-0000-4000-8000-%';

DELETE FROM game_service.xp_ledgers
 WHERE source_type = 'QUEST'
   AND source_id::text LIKE '66666666-0000-4000-8000-%';

DELETE FROM game_service.quests
 WHERE quest_id::text LIKE '66666666-0000-4000-8000-%';

DELETE FROM game_service.outbox_events
 WHERE aggregate_type = 'PARTY_QUEST'
   AND aggregate_id::text LIKE '22222222-0000-4000-8000-%';

DELETE FROM game_service.xp_ledgers
 WHERE source_type = 'PARTY_QUEST'
   AND source_id::text LIKE '22222222-0000-4000-8000-%';

DELETE FROM game_service.wallets
 WHERE user_id::text LIKE '33333333-0000-4000-8000-%';

DELETE FROM game_service.party_quest_members
 WHERE party_quest_id::text LIKE '22222222-0000-4000-8000-%';

DELETE FROM game_service.party_quests
 WHERE party_quest_id::text LIKE '22222222-0000-4000-8000-%';

DELETE FROM game_service.user_latest_snapshots
 WHERE user_id::text LIKE '33333333-0000-4000-8000-%';

DELETE FROM game_service.parties
 WHERE id::text LIKE '11111111-0000-4000-8000-%';

COMMIT;
