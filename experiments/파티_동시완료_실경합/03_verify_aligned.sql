-- ============================================================
-- 파티 실경합 실험 검증 (멤버 수와 상관없이 쓰도록 "파티별 멤버 수"와 비교한다)
-- 실험 데이터만 본다: 파티 퀘스트 22222222-… / 유저 33333333-…
-- 기대값은 gen_aligned.py 가 출력한다. 기본값(파티 100 × 멤버 4 · 목표 3000): 1) 100 / 0 / 100  2) 400건 · 12000  3) 0행  4) 0행  5) 100  6) 0행  참고) 1000 × 400
-- 4) 가 0행이면 완료 뒤에 도착한 네 번째 기여까지 카운터에 더해졌다는 뜻이다 (카운터 = 기여 합 = 4000)
-- ============================================================

\echo '=== 1. 완료 / 미완 / 전체 ==='
SELECT count(*) FILTER (WHERE status = 'COMPLETED') AS 완료,
       count(*) FILTER (WHERE status = 'ACTIVE')    AS 미완,
       count(*)                                     AS 전체
  FROM game_service.party_quests
 WHERE party_quest_id::text LIKE '22222222-0000-4000-8000-%';

\echo '=== 2. XP 원장 건수 · 총액 (많으면 중복 지급) ==='
SELECT count(*) AS 원장_행수, coalesce(sum(amount), 0) AS 지급_총액
  FROM game_service.xp_ledgers
 WHERE source_type = 'PARTY_QUEST'
   AND source_id::text LIKE '22222222-0000-4000-8000-%';

\echo '=== 3. 원장 수 <> 멤버 수 인 파티 (0행이어야 한다) ==='
SELECT q.party_quest_id, q.status, coalesce(r.rewards, 0) AS 원장, m.members AS 멤버
  FROM game_service.party_quests q
  JOIN (SELECT party_quest_id, count(*) AS members
          FROM game_service.party_quest_members GROUP BY party_quest_id) m
    ON m.party_quest_id = q.party_quest_id
  LEFT JOIN (SELECT source_id, count(*) AS rewards
               FROM game_service.xp_ledgers WHERE source_type = 'PARTY_QUEST'
              GROUP BY source_id) r
    ON r.source_id = q.party_quest_id
 WHERE q.party_quest_id::text LIKE '22222222-0000-4000-8000-%'
   AND coalesce(r.rewards, 0) <> m.members;

\echo '=== 4. 카운터 <> 멤버 기여 합 인 파티 (0행이어야 한다) ==='
SELECT q.party_quest_id, q.current_val AS 카운터, sum(m.contributed_val) AS 기여합계
  FROM game_service.party_quests q
  JOIN game_service.party_quest_members m ON m.party_quest_id = q.party_quest_id
 WHERE q.party_quest_id::text LIKE '22222222-0000-4000-8000-%'
 GROUP BY q.party_quest_id, q.current_val
HAVING q.current_val <> sum(m.contributed_val);

\echo '=== 5. 완료 이벤트(Outbox) 건수 (파티 수와 같아야 한다) ==='
SELECT count(*) AS 완료이벤트
  FROM game_service.outbox_events
 WHERE event_type = 'PARTY_QUEST_COMPLETED'
   AND aggregate_id::text LIKE '22222222-0000-4000-8000-%';

\echo '=== 6. 지갑 <> 원장 합계 인 유저 (0행이어야 한다) ==='
SELECT coalesce(w.user_id, l.user_id) AS user_id, w.xp AS 지갑, l.total AS 원장합계
  FROM (SELECT user_id, sum(amount) AS total FROM game_service.xp_ledgers
         WHERE user_id::text LIKE '33333333-0000-4000-8000-%' GROUP BY user_id) l
  FULL OUTER JOIN (SELECT * FROM game_service.wallets
                    WHERE user_id::text LIKE '33333333-0000-4000-8000-%') w
    ON w.user_id = l.user_id
 WHERE coalesce(w.xp, 0) <> coalesce(l.total, 0);

\echo '=== 참고: 멤버 기여분 분포 (목표 = 인원 × 1000 이면 전부 1000. 4명 · 3000 이면 0 이 섞일 수 있다: 완료가 커밋된 뒤 시작한 이벤트는 퀘스트가 이미 ACTIVE 가 아니라 통째로 무시된다) ==='
SELECT contributed_val, count(*)
  FROM game_service.party_quest_members
 WHERE party_quest_id::text LIKE '22222222-0000-4000-8000-%'
 GROUP BY contributed_val ORDER BY contributed_val;

\echo '=== 참고: 완료 뒤 기여의 두 갈래 (4명 · 3000 일 때) ==='
-- 카운터 4000 = 네 번째 기여가 완료 직전에 시작해 완료 뒤에 더해진 파티 (트레이드오프 A)
-- 카운터 3000 = 네 번째 이벤트가 완료 뒤에 시작해 무시된 파티 (멤버 하나가 기여 0)
SELECT q.current_val AS 카운터, count(*) AS 파티수,
       sum(CASE WHEN z.zeros IS NULL THEN 0 ELSE z.zeros END) AS 기여0_멤버수
  FROM game_service.party_quests q
  LEFT JOIN (SELECT party_quest_id, count(*) AS zeros FROM game_service.party_quest_members
              WHERE contributed_val = 0 GROUP BY party_quest_id) z ON z.party_quest_id = q.party_quest_id
 WHERE q.party_quest_id::text LIKE '22222222-0000-4000-8000-%'
 GROUP BY q.current_val ORDER BY q.current_val;
