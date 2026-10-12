"""B-19 발행측 '최소 한 번' — 데이터 생성기 (2026-10-12)

질문: Outbox 발행 도중 앱이 죽으면(Kafka 에는 나갔는데 PUBLISHED 표시는 커밋 전) 이벤트가 사라지는가, 몇 건이 중복되는가
  1) 발행을 막아 둔 채(publish-fail-flag) 유저 50명의 퀘스트를 완료시켜 Outbox 50행을 PENDING 으로 쌓는다
  2) 막은 것을 풀면서 crash-after-send-flag 를 켠다 -> 다음 라운드가 10건째를 보낸 직후 JVM 을 즉시 멈춘다(kill -9)
  3) 플래그 없이 재기동 -> PENDING 50행을 다시 집어 전부 보낸다
  기대: game.events 에서 서로 다른 eventId 50(유실 0) · 레코드 60(중복 10, 같은 eventId · 같은 payload) · 최종 미발행 0
유저마다 개인 퀘스트 1개(기준 1000 · 목표 500 · 보상 10), 이벤트 1건(누적 2000)으로 완료
식별자 앞자리 b19: 유저 b1900001 / 퀘스트 b1900002 / 이벤트 b1900009
"""
import os

here = os.path.dirname(os.path.abspath(__file__))
U = lambda pfx, n: f"{pfx}-0000-4000-8000-{n:012d}"
TODAY_END = "((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'"
N = 50


def ev(i):
    u = U("b1900001", i)
    return (f'{u}|{{"eventId":"{U("b1900009", i)}","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"{u}",'
            f'"data":{{"activityDate":"__TODAY__","measuredAt":"__NOW__","cumulative":{{"steps":2000,"activeMinutes":0,"activeCalories":0}}}}}}')


seed = f"""-- B-19 시드 (gen_b19.py 가 만듦)
DO $$ BEGIN
  IF (now() AT TIME ZONE 'Asia/Seoul')::time > time '23:30' THEN RAISE EXCEPTION '자정 근처다. 23:30 전에 하자'; END IF;
END $$;
BEGIN;
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
SELECT ('b1900002-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('b1900001-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       gen_random_uuid(), 'B19 personal ' || i, 'STEPS', 500, 1000,
       now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, {TODAY_END}, 0
  FROM generate_series(1, {N}) AS i;
COMMIT;
"""

cleanup = """-- B-19 데이터만 지운다
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b190000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'b190000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'b190000%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'b1900002%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'b190000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'b190000%';
COMMIT;
"""

files = {"01_seed_b19.sql": seed, "99_cleanup_b19.sql": cleanup,
         "02_events.template.txt": "\n".join(ev(i) for i in range(1, N + 1)) + "\n"}
for name, body in files.items():
    with open(os.path.join(here, name), "w", encoding="utf-8", newline="\n") as f:
        f.write(body)
print(f"유저 {N}명 · 퀘스트 {N}개 · 이벤트 {N}건 (b19)")
