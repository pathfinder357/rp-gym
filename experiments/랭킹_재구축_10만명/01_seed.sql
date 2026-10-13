-- ============================================================
-- 랭킹 재구축 10만 명 랭킹 재구축 성능 실험 시드 (2026-10-11)
-- 사용자 10만 명: characters 1행 + wallets 1행. 식별자 앞자리 유저 88888888 / 캐릭터 99999999
-- xp = (i * 37) % 50000 (0 ~ 49,999 고르게), level = 1 + xp / 100 (LevelPolicy 와 같다)
-- 앞부분에서 지난 회차 데이터를 먼저 지운다(같은 파일로 정리 + 생성)
-- ============================================================
BEGIN;

DELETE FROM game_service.wallets    WHERE user_id::text LIKE '88888888-0000-4000-8000-%';
DELETE FROM game_service.characters WHERE user_id::text LIKE '88888888-0000-4000-8000-%';

INSERT INTO game_service.characters (id, user_id, level, created_at, updated_at)
SELECT ('99999999-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('88888888-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       1 + ((i * 37) % 50000) / 100,
       now(), now()
  FROM generate_series(1, 100000) AS i;

INSERT INTO game_service.wallets (user_id, xp)
SELECT ('88888888-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid, (i * 37) % 50000
  FROM generate_series(1, 100000) AS i;

COMMIT;

ANALYZE game_service.characters;
ANALYZE game_service.wallets;

SELECT (SELECT count(*) FROM game_service.characters WHERE user_id::text LIKE '88888888-0000-4000-8000-%') AS a10_characters,
       (SELECT count(*) FROM game_service.wallets    WHERE user_id::text LIKE '88888888-0000-4000-8000-%') AS a10_wallets,
       (SELECT count(*) FROM game_service.characters) AS all_characters;
-- 100000 / 100000 이어야 한다
