-- ============================================================
-- 동시성 실험 검증 — 다섯 가지를 본다
--
-- 전부 통과해야 "돈이 정확하다" 고 말할 수 있다.
-- 하나라도 어긋나면 그 항목의 해석은 README 에 적어뒀다.
-- ============================================================

\echo '=== 1. 완료가 정확히 100건인가 ==='
-- 적으면 완료를 놓친 것이고, 많을 수 없다 (파티당 행이 하나다).
SELECT count(*) FILTER (WHERE status = 'COMPLETED') AS 완료,
       count(*) FILTER (WHERE status = 'ACTIVE')    AS 미완,
       count(*)                                     AS 전체
  FROM game_service.party_quests;

\echo '=== 2. XP 원장이 정확히 400건인가 ==='
-- 이 숫자가 400보다 크면 중복 지급이다. 완료 선점이 뚫렸다는 뜻이다.
SELECT count(*) AS 원장_행수,
       sum(amount) AS 지급_총액
  FROM game_service.xp_ledgers
 WHERE source_type = 'PARTY_QUEST';

\echo '=== 3. 파티당 정확히 4건인가 (한 건이라도 어긋난 파티) ==='
-- 0행이어야 한다. 행이 나오면 그 파티에서 누가 못 받았거나 두 번 받은 것이다.
SELECT source_id AS 파티퀘스트, count(*) AS 지급건수
  FROM game_service.xp_ledgers
 WHERE source_type = 'PARTY_QUEST'
 GROUP BY source_id
HAVING count(*) <> 4;

\echo '=== 4. 정합성 등식이 성립하는가 (깨진 파티) ==='
-- current_val == SUM(contributed_val)
-- 0행이어야 한다. 행이 나오면 공유 카운터에서 누군가의 갱신이 사라진 것이다.
SELECT q.party_quest_id AS 파티퀘스트,
       q.current_val    AS 카운터,
       sum(m.contributed_val) AS 기여합계,
       q.current_val - sum(m.contributed_val) AS 차이
  FROM game_service.party_quests q
  JOIN game_service.party_quest_members m ON m.party_quest_id = q.party_quest_id
 GROUP BY q.party_quest_id, q.current_val
HAVING q.current_val <> sum(m.contributed_val);

\echo '=== 5. 완료 이벤트가 파티당 1건인가 ==='
-- 100이어야 한다. 아웃박스의 유니크 제약이 2차 방어선이라 이 값이 100을 넘을 수는 없다.
-- 100보다 작으면 완료는 됐는데 알림이 안 나간 파티가 있다는 뜻이다.
SELECT count(*) AS 완료이벤트
  FROM game_service.outbox_events
 WHERE event_type = 'PARTY_QUEST_COMPLETED';

\echo '=== 6. 지갑과 원장이 맞는가 (어긋난 유저) ==='
-- 0행이어야 한다. wallets.xp 는 원장 합계로 다시 계산할 수 있는 파생값이다.
SELECT w.user_id, w.xp AS 지갑, l.합계
  FROM game_service.wallets w
  JOIN (SELECT user_id, sum(amount) AS 합계
          FROM game_service.xp_ledgers GROUP BY user_id) l
    ON l.user_id = w.user_id
 WHERE w.xp <> l.합계;

\echo '=== 참고: 멤버 기여분이 전부 1000인가 ==='
-- 1000이 아닌 행이 있으면 델타 계산이나 워터마크가 의심스럽다.
SELECT contributed_val, count(*)
  FROM game_service.party_quest_members
 GROUP BY contributed_val ORDER BY contributed_val;
