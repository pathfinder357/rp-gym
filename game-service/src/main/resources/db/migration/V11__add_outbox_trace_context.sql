-- ============================================================
-- V11 --  Outbox 를 거쳐도 trace 가 이어지게 한다
--
-- 요청 · 컨슈머 스레드의 trace 는 Outbox 에 적재하는 순간 끝나고, 폴링 스레드가 새 trace 로 발행했다.
-- 그래서 Health -> Game 소비 -> Game 발행 -> Notification 이 한 trace 로 보이지 않았다.
-- 적재할 때 현재 trace context 를 전파 헤더 형태(JSON, 예: {"traceparent":"00-...-...-01"})로 함께 저장하고,
-- 발행할 때 그 값을 부모로 span 을 열어 Kafka 헤더로 다시 싣는다.
--
-- NULL 허용: 이 컬럼 이전에 적재된 행, trace 가 없는 경로(스케줄러 등)에서 적재된 행은 비어 있다.
-- 비어 있으면 예전처럼 새 trace 로 발행한다.
-- ============================================================
ALTER TABLE game_service.outbox_events
    ADD COLUMN trace_context VARCHAR(512);
