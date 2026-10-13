\echo '=== P (PostgreSQL 60s): users 1~150 (expect completed 150, ledger rows 150, distinct quests 150, xp 1500, outbox 150 all PUBLISHED) ==='
SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a1600002%' AND substr(quest_id::text, 25)::bigint BETWEEN 1 AND 150 AND status = 'COMPLETED') AS completed,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' AND substr(user_id::text, 25)::bigint BETWEEN 1 AND 150) AS ledger_rows,
       (SELECT count(DISTINCT source_id) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' AND substr(user_id::text, 25)::bigint BETWEEN 1 AND 150) AS distinct_quests,
       (SELECT coalesce(sum(amount), 0) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' AND substr(user_id::text, 25)::bigint BETWEEN 1 AND 150) AS xp,
       (SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a1600002%' AND substr(aggregate_id::text, 25)::bigint BETWEEN 1 AND 150) AS outbox,
       (SELECT count(*) FILTER (WHERE status <> 'PUBLISHED') FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a1600002%' AND substr(aggregate_id::text, 25)::bigint BETWEEN 1 AND 150) AS outbox_not_published;
\echo '=== K (Kafka 90s): users 201~350 (expect completed 150, ledger rows 150, distinct quests 150, xp 1500, outbox 150 all PUBLISHED) ==='
SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a1600002%' AND substr(quest_id::text, 25)::bigint BETWEEN 201 AND 350 AND status = 'COMPLETED') AS completed,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' AND substr(user_id::text, 25)::bigint BETWEEN 201 AND 350) AS ledger_rows,
       (SELECT count(DISTINCT source_id) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' AND substr(user_id::text, 25)::bigint BETWEEN 201 AND 350) AS distinct_quests,
       (SELECT coalesce(sum(amount), 0) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' AND substr(user_id::text, 25)::bigint BETWEEN 201 AND 350) AS xp,
       (SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a1600002%' AND substr(aggregate_id::text, 25)::bigint BETWEEN 201 AND 350) AS outbox,
       (SELECT count(*) FILTER (WHERE status <> 'PUBLISHED') FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a1600002%' AND substr(aggregate_id::text, 25)::bigint BETWEEN 201 AND 350) AS outbox_not_published;

\echo '=== all a16 users: wallet <> ledger (expect 0 rows) ==='
SELECT coalesce(w.user_id, l.user_id) AS user_id, w.xp AS wallet, l.total AS ledger
  FROM (SELECT user_id, sum(amount) AS total FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' GROUP BY user_id) l
  FULL OUTER JOIN (SELECT * FROM game_service.wallets WHERE user_id::text LIKE 'a1600001%') w ON w.user_id = l.user_id
 WHERE coalesce(w.xp, 0) <> coalesce(l.total, 0);
\echo '=== users with more than one ledger row (expect 0 rows = no double grant) ==='
SELECT user_id, count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a1600001%' GROUP BY user_id HAVING count(*) > 1;
