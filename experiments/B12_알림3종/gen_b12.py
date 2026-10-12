"""B-12 알림 3종 실험 — 데이터 생성기 (2026-10-12)

1) DLT 적재 알림      : 깨진 JSON 1건 -> 바로 DLT -> Game Consumer DLT Detected
2) 컨슈머 지연 알림    : 유저 b1200002-..001 의 이벤트 6건(같은 키 = 같은 파티션). 앱은 stuck-flag 파일이 있는 동안
                        이 유저 이벤트에서 '일시 장애' 예외 -> 2초 간격 무한 재시도 -> 그 파티션이 멈추고 뒤 5건이 lag
                        -> 10분 동안 lag 이 0 으로 안 내려오면 Game Consumer Lag Stuck. 파일을 지우면 풀림
3) Outbox 최장 대기 알림: 개인 퀘스트 1개(기준 1000 · 목표 500)를 완료시키는 이벤트 1건. 앱은 publish-fail-flag 파일이
                        있는 동안 발행을 실패시킨다 -> QUEST_COMPLETED 가 PENDING 으로 남음 -> 60초 초과가 2분 지속되면
                        Game Outbox Oldest Pending High. 파일을 지우면 다음 폴링에서 발행
식별자 앞자리 b12: 유저 b1200001(깨진 JSON 키) · b1200002(지연) · b1200003(Outbox) / 퀘스트 b1200004 / 이벤트 b1200009
"""
import os

here = os.path.dirname(os.path.abspath(__file__))
U = lambda pfx, n: f"{pfx}-0000-4000-8000-{n:012d}"
TODAY_END = "((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'"


def ev(eid, user, steps):
    return (f'{user}|{{"eventId":"{U("b1200009", eid)}","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"{user}",'
            f'"data":{{"activityDate":"__TODAY__","measuredAt":"__NOW__","cumulative":{{"steps":{steps},"activeMinutes":0,"activeCalories":0}}}}}}')


seed = f"""-- B-12 시드 (gen_b12.py 가 만듦)
BEGIN;
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
VALUES ('{U("b1200004", 1)}', '{U("b1200003", 1)}', gen_random_uuid(), 'B12 outbox quest', 'STEPS', 500, 1000,
        now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, {TODAY_END}, 0);
COMMIT;
SELECT count(*) AS b12_quests FROM game_service.quests WHERE quest_id::text LIKE 'b1200004%';
"""
p1 = f'{U("b1200001", 1)}|{{"eventId": broken json for B-12\n'
p2 = "\n".join(ev(100 + i, U("b1200002", 1), 1000 + i * 100) for i in range(1, 7)) + "\n"
p3 = ev(300, U("b1200003", 1), 2000) + "\n"
cleanup = """-- B-12 데이터만 지운다
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b120000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'b120000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'b120000%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'b1200004%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'b120000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'b120000%';
COMMIT;
"""
for name, body in [("01_seed_b12.sql", seed), ("02_p1_dlt.template.txt", p1), ("03_p2_lag.template.txt", p2),
                   ("04_p3_outbox.template.txt", p3), ("99_cleanup_b12.sql", cleanup)]:
    with open(os.path.join(here, name), "w", encoding="utf-8", newline="\n") as f:
        f.write(body)
print("만든 파일: 01_seed_b12.sql, 02_p1_dlt / 03_p2_lag / 04_p3_outbox .template.txt, 99_cleanup_b12.sql")
