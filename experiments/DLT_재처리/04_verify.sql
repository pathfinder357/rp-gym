-- DLT 재처리 검증 (기대값은 각 제목 괄호 안)
\echo '=== F. bug -> DLT -> fix -> replay (expect COMPLETED 10, ledger 10 rows / 100 xp) ==='
SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a6000002-0000-4000-8000-0000000001%' AND status = 'COMPLETED') AS completed,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a6000001-0000-4000-8000-0000000001%') AS ledger_rows,
       (SELECT coalesce(sum(amount), 0) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a6000001-0000-4000-8000-0000000001%') AS ledger_xp;
\echo '=== D. already processed, replayed again (expect COMPLETED 10, ledger still 10 rows / 100 xp) ==='
SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a6000002-0000-4000-8000-0000000002%' AND status = 'COMPLETED') AS completed,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a6000001-0000-4000-8000-0000000002%') AS ledger_rows,
       (SELECT coalesce(sum(amount), 0) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a6000001-0000-4000-8000-0000000002%') AS ledger_xp;
\echo '=== Y. yesterday events replayed (expect ACTIVE 10, last_applied NULL 10, ledger 0) ==='
SELECT count(*) FILTER (WHERE status = 'ACTIVE') AS active,
       count(*) FILTER (WHERE last_applied_measured_at IS NULL) AS watermark_null,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a6000001-0000-4000-8000-0000000003%') AS ledger_rows
  FROM game_service.quests WHERE quest_id::text LIKE 'a6000002-0000-4000-8000-0000000003%';
\echo '=== S. expired suggestions replayed (expect stored 0, card 0) ==='
SELECT (SELECT count(*) FROM game_service.quest_suggestions WHERE suggestion_id::text LIKE 'a6000008%') AS stored,
       (SELECT count(*) FROM game_service.outbox_events WHERE event_type = 'QUEST_SUGGESTED' AND aggregate_id::text LIKE 'a6000008%') AS card;
\echo '=== all a6 users: wallet <> ledger sum (expect 0 rows) ==='
SELECT coalesce(w.user_id, l.user_id) AS user_id, w.xp AS wallet, l.total AS ledger
  FROM (SELECT user_id, sum(amount) AS total FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a6000001%' GROUP BY user_id) l
  FULL OUTER JOIN (SELECT * FROM game_service.wallets WHERE user_id::text LIKE 'a6000001%') w ON w.user_id = l.user_id
 WHERE coalesce(w.xp, 0) <> coalesce(l.total, 0);
\echo '=== QUEST_COMPLETED events in outbox (expect 20: F 10 + D 10) ==='
SELECT count(*) AS quest_completed FROM game_service.outbox_events WHERE event_type = 'QUEST_COMPLETED' AND aggregate_id::text LIKE 'a6000002%';
