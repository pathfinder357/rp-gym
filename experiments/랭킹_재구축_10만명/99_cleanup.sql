-- 랭킹 재구축 10만 명 실험 데이터 정리 (실험 끝에 실행기가 부른다). 랭킹 쪽은 실행기가 Lua 로 지운다
BEGIN;
DELETE FROM game_service.wallets    WHERE user_id::text LIKE '88888888-0000-4000-8000-%';
DELETE FROM game_service.characters WHERE user_id::text LIKE '88888888-0000-4000-8000-%';
COMMIT;
