\echo '=== 1) one broker down: users 1~100 (expect completed 100, ledger rows 100, xp 1000, outbox 100 all PUBLISHED, users over 1 ledger row 0) ==='
SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'c1000002%' AND substr(quest_id::text, 25)::bigint BETWEEN 1 AND 100 AND status = 'COMPLETED') AS completed,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c1000001%' AND substr(user_id::text, 25)::bigint BETWEEN 1 AND 100) AS ledger_rows,
       (SELECT coalesce(sum(amount), 0) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c1000001%' AND substr(user_id::text, 25)::bigint BETWEEN 1 AND 100) AS xp,
       (SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'c1000002%' AND substr(aggregate_id::text, 25)::bigint BETWEEN 1 AND 100) AS outbox,
       (SELECT count(*) FILTER (WHERE status <> 'PUBLISHED') FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'c1000002%' AND substr(aggregate_id::text, 25)::bigint BETWEEN 1 AND 100) AS outbox_not_published,
       (SELECT count(*) FROM (SELECT user_id FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c1000001%' AND substr(user_id::text, 25)::bigint BETWEEN 1 AND 100 GROUP BY user_id HAVING count(*) > 1) x) AS users_over_1;
\echo '=== 2) two brokers down: users 201~220 (expect completed 20, ledger rows 20, xp 200, outbox 20 all PUBLISHED, users over 1 ledger row 0) ==='
SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'c1000002%' AND substr(quest_id::text, 25)::bigint BETWEEN 201 AND 220 AND status = 'COMPLETED') AS completed,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c1000001%' AND substr(user_id::text, 25)::bigint BETWEEN 201 AND 220) AS ledger_rows,
       (SELECT coalesce(sum(amount), 0) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c1000001%' AND substr(user_id::text, 25)::bigint BETWEEN 201 AND 220) AS xp,
       (SELECT count(*) FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'c1000002%' AND substr(aggregate_id::text, 25)::bigint BETWEEN 201 AND 220) AS outbox,
       (SELECT count(*) FILTER (WHERE status <> 'PUBLISHED') FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'c1000002%' AND substr(aggregate_id::text, 25)::bigint BETWEEN 201 AND 220) AS outbox_not_published,
       (SELECT count(*) FROM (SELECT user_id FROM game_service.xp_ledgers WHERE user_id::text LIKE 'c1000001%' AND substr(user_id::text, 25)::bigint BETWEEN 201 AND 220 GROUP BY user_id HAVING count(*) > 1) x) AS users_over_1;
