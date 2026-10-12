-- S0 가벼운 끝: 퀘스트도 파티 퀘스트도 없는 유저 n 명.
-- 스냅샷 행만 30분 전 값으로 미리 넣는다. 실제로는 모든 유저가 30분마다 동기화하므로
-- 정각 이벤트는 새 행 INSERT 가 아니라 기존 행 UPDATE 경로를 탄다. 그 경로를 재려는 것이다.
-- 변수: n, prefix (userId 앞 24자리)
INSERT INTO game_service.user_latest_snapshots
  (user_id, activity_date, measured_at, steps, active_minutes, active_calories, created_at, updated_at)
SELECT (:'prefix' || lpad(g::text, 12, '0'))::uuid,
       ((now() - interval '30 minutes') AT TIME ZONE 'Asia/Seoul')::date,
       now() - interval '30 minutes',
       1000, 10, 50,
       now() AT TIME ZONE 'UTC', now() AT TIME ZONE 'UTC'
FROM generate_series(1, :n) g;
SELECT count(*) AS seeded FROM game_service.user_latest_snapshots
 WHERE user_id BETWEEN (:'prefix' || '000000000000')::uuid AND (:'prefix' || 'ffffffffffff')::uuid;
