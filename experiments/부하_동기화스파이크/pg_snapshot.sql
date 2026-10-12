-- 분해용 스냅샷. 앞뒤로 두 번 찍어 차이를 본다. 설정 변경 없이 읽기만 한다.
SELECT 'db', 'xact_commit',   xact_commit   FROM pg_stat_database WHERE datname = current_database()
UNION ALL SELECT 'db', 'xact_rollback', xact_rollback FROM pg_stat_database WHERE datname = current_database()
UNION ALL SELECT 'db', 'tup_returned',  tup_returned  FROM pg_stat_database WHERE datname = current_database()
UNION ALL SELECT 'db', 'tup_fetched',   tup_fetched   FROM pg_stat_database WHERE datname = current_database()
UNION ALL SELECT 'db', 'tup_inserted',  tup_inserted  FROM pg_stat_database WHERE datname = current_database()
UNION ALL SELECT 'db', 'tup_updated',   tup_updated   FROM pg_stat_database WHERE datname = current_database()
UNION ALL SELECT 'db', 'blks_read',     blks_read     FROM pg_stat_database WHERE datname = current_database()
UNION ALL SELECT relname, 'seq_scan',  coalesce(seq_scan, 0)  FROM pg_stat_user_tables WHERE schemaname = 'game_service'
UNION ALL SELECT relname, 'idx_scan',  coalesce(idx_scan, 0)  FROM pg_stat_user_tables WHERE schemaname = 'game_service'
UNION ALL SELECT relname, 'n_tup_ins', n_tup_ins              FROM pg_stat_user_tables WHERE schemaname = 'game_service'
UNION ALL SELECT relname, 'n_tup_upd', n_tup_upd              FROM pg_stat_user_tables WHERE schemaname = 'game_service';
