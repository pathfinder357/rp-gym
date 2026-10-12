-- A-8 검증
\echo '=== quests (expect COMPLETED 10, version = 5 conflicts + 1 own save = 6) ==='
SELECT status, count(*), min(version) AS min_version, max(version) AS max_version
  FROM game_service.quests WHERE quest_id::text LIKE 'a8000002%' GROUP BY status;
\echo '=== ledger (expect 10 rows / 100 xp, no duplicates) ==='
SELECT count(*) AS rows, coalesce(sum(amount), 0) AS xp, count(DISTINCT source_id) AS distinct_quests
  FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a8000001%';
\echo '=== wallet <> ledger (expect 0 rows) ==='
SELECT coalesce(w.user_id, l.user_id) AS user_id, w.xp AS wallet, l.total AS ledger
  FROM (SELECT user_id, sum(amount) AS total FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a8000001%' GROUP BY user_id) l
  FULL OUTER JOIN (SELECT * FROM game_service.wallets WHERE user_id::text LIKE 'a8000001%') w ON w.user_id = l.user_id
 WHERE coalesce(w.xp, 0) <> coalesce(l.total, 0);
\echo '=== QUEST_COMPLETED events (expect 10) ==='
SELECT count(*) FROM game_service.outbox_events WHERE event_type = 'QUEST_COMPLETED' AND aggregate_id::text LIKE 'a8000002%';
