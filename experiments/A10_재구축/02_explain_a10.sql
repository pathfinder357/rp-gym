-- A-10 / R-22: 재구축 원천 쿼리 실행 계획. 10만 명 시드 뒤, 중간 지점(5만 번째) 다음 1,000명을 읽을 때
\echo '=== 예전 쿼리 (characters UNION wallets 를 먼저 만든 뒤 고른다) ==='
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT u.user_id AS userId, COALESCE(w.xp, 0) AS totalXp
  FROM (SELECT user_id FROM game_service.characters
        UNION
        SELECT user_id FROM game_service.wallets) u
  LEFT JOIN game_service.wallets w ON w.user_id = u.user_id
 WHERE u.user_id > '88888888-0000-4000-8000-000000050000'
 ORDER BY u.user_id
 LIMIT 1000;

\echo '=== 지금 쿼리 (R-22: 테이블마다 인덱스로 1,000명씩 읽고 합친다) ==='
EXPLAIN (ANALYZE, BUFFERS, COSTS OFF)
SELECT t.user_id AS userId, COALESCE(MAX(t.xp), 0) AS totalXp
  FROM ((SELECT user_id, NULL::integer AS xp
           FROM game_service.characters
          WHERE user_id > '88888888-0000-4000-8000-000000050000'
          ORDER BY user_id
          LIMIT 1000)
        UNION ALL
        (SELECT user_id, xp
           FROM game_service.wallets
          WHERE user_id > '88888888-0000-4000-8000-000000050000'
          ORDER BY user_id
          LIMIT 1000)) t
 GROUP BY t.user_id
 ORDER BY t.user_id
 LIMIT 1000;
