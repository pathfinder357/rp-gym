-- 늦게 온 이벤트 검증 (기대값은 각 제목 괄호 안)
\echo '=== P-A. yesterday-only personal quests (expect ACTIVE 25, watermark NULL 25, reward 0) ==='
SELECT count(*) FILTER (WHERE status = 'ACTIVE') AS active,
       count(*) FILTER (WHERE last_applied_measured_at IS NULL) AS watermark_null,
       (SELECT count(*) FROM game_service.xp_ledgers l WHERE l.source_type = 'QUEST'
           AND l.source_id IN (SELECT quest_id FROM game_service.quests WHERE quest_id::text LIKE 'a5000002%'
                                AND user_id::text <= 'a5000001-0000-4000-8000-000000000025')) AS rewards
  FROM game_service.quests
 WHERE quest_id::text LIKE 'a5000002%' AND user_id::text <= 'a5000001-0000-4000-8000-000000000025';

\echo '=== P-B. yesterday then today (expect COMPLETED 25, last_cumulative 6000 x25, 8000 x0, reward 25) ==='
SELECT count(*) FILTER (WHERE status = 'COMPLETED') AS completed,
       count(*) FILTER (WHERE last_cumulative_val = 6000) AS cum_6000,
       count(*) FILTER (WHERE last_cumulative_val = 8000) AS cum_8000,
       (SELECT count(*) FROM game_service.xp_ledgers l WHERE l.source_type = 'QUEST'
           AND l.source_id IN (SELECT quest_id FROM game_service.quests WHERE quest_id::text LIKE 'a5000002%'
                                AND user_id::text > 'a5000001-0000-4000-8000-000000000025')) AS rewards
  FROM game_service.quests
 WHERE quest_id::text LIKE 'a5000002%' AND user_id::text > 'a5000001-0000-4000-8000-000000000025';

\echo '=== Q. party (expect COMPLETED 25, counter 4000 x25, contribution 1000 x100, reward 100) ==='
SELECT (SELECT count(*) FROM game_service.party_quests WHERE party_quest_id::text LIKE 'a5000004%' AND status = 'COMPLETED') AS completed,
       (SELECT count(*) FROM game_service.party_quests WHERE party_quest_id::text LIKE 'a5000004%' AND current_val = 4000) AS counter_4000,
       (SELECT count(*) FROM game_service.party_quest_members WHERE party_quest_id::text LIKE 'a5000004%' AND contributed_val = 1000) AS contrib_1000,
       (SELECT count(*) FROM game_service.party_quest_members WHERE party_quest_id::text LIKE 'a5000004%' AND contributed_val > 1000) AS contrib_over,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE source_type = 'PARTY_QUEST' AND source_id::text LIKE 'a5000004%') AS rewards;

\echo '=== S. suggestions (expect expired_31m stored 0 / cards 0, control_29m stored 10 / cards 10) ==='
SELECT (SELECT count(*) FROM game_service.quest_suggestions WHERE suggestion_id::text LIKE 'a5000008%'
           AND suggestion_id::text <= 'a5000008-0000-4000-8000-000000000010') AS expired_31m_stored,
       (SELECT count(*) FROM game_service.outbox_events WHERE event_type = 'QUEST_SUGGESTED' AND aggregate_id::text LIKE 'a5000008%'
           AND aggregate_id::text <= 'a5000008-0000-4000-8000-000000000010') AS expired_31m_cards,
       (SELECT count(*) FROM game_service.quest_suggestions WHERE suggestion_id::text LIKE 'a5000008%'
           AND suggestion_id::text > 'a5000008-0000-4000-8000-000000000010') AS control_29m_stored,
       (SELECT count(*) FROM game_service.outbox_events WHERE event_type = 'QUEST_SUGGESTED' AND aggregate_id::text LIKE 'a5000008%'
           AND aggregate_id::text > 'a5000008-0000-4000-8000-000000000010') AS control_29m_cards;
