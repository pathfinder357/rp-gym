"""trace 이어짐 trace 이어짐 — 데이터 생성기 (2026-10-12)

질문: 이벤트 하나가 Game 에 들어와서(소비) 퀘스트 완료 -> Outbox 적재 -> (다른 스레드) Outbox 발행까지
      하나의 trace 로 이어지는가. 그리고 받은 trace 를 발행 레코드 헤더로 다음 서비스(Notification)에 넘기는가.
  - Health · Notification 은 담당 밖이라 띄우지 않는다. 대신
    입구: Health 가 보냈을 W3C traceparent 헤더를 직접 붙여 넣는다 (traceId 고정 b17000...0001)
    출구: game.events 레코드 헤더의 traceparent 를 읽는다 = Notification 이 이어 받을 값
  - 대조: 헤더 없이 넣은 1건은 Game 소비에서 새 trace 가 시작돼야 한다
유저마다 개인 퀘스트 1개(기준 1000 · 목표 500 · 보상 10), 이벤트 1건(누적 2000)으로 완료
식별자 앞자리 b17: 유저 b1700001 / 퀘스트 b1700002 / 이벤트 b1700009
"""
import os

here = os.path.dirname(os.path.abspath(__file__))
U = lambda pfx, n: f"{pfx}-0000-4000-8000-{n:012d}"
TODAY_END = "((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'"
TRACE_ID = "b1700000000000000000000000000001"
PARENT_SPAN = "b170000000000001"
TRACEPARENT = f"00-{TRACE_ID}-{PARENT_SPAN}-01"


def ev(i):
    u = U("b1700001", i)
    return (f'{u}|{{"eventId":"{U("b1700009", i)}","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"{u}",'
            f'"data":{{"activityDate":"__TODAY__","measuredAt":"__NOW__","cumulative":{{"steps":2000,"activeMinutes":0,"activeCalories":0}}}}}}')


seed = f"""-- trace 이어짐 시드 (gen.py 가 만듦)
DO $$ BEGIN
  IF (now() AT TIME ZONE 'Asia/Seoul')::time > time '23:30' THEN RAISE EXCEPTION '자정 근처다. 23:30 전에 하자'; END IF;
END $$;
BEGIN;
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
SELECT ('b1700002-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('b1700001-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       gen_random_uuid(), 'B17 personal ' || i, 'STEPS', 500, 1000,
       now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, {TODAY_END}, 0
  FROM unnest(ARRAY[1, 2]) AS i;
COMMIT;
"""

cleanup = """-- trace 이어짐 데이터만 지운다
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'b170000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'b170000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'b170000%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'b1700002%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'b170000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'b170000%';
COMMIT;
"""

files = {"01_seed.sql": seed, "99_cleanup.sql": cleanup,
         # 헤더 붙인 줄: 헤더 <TAB> key|value  (kafka-console-producer parse.headers)
         "02_traced.template.txt": f"traceparent:{TRACEPARENT}\t{ev(1)}\n",
         "03_plain.template.txt": ev(2) + "\n"}
for name, body in files.items():
    with open(os.path.join(here, name), "w", encoding="utf-8", newline="\n") as f:
        f.write(body)
print(f"traceparent 붙인 1건(유저 ...001, traceId {TRACE_ID}, 부모 span {PARENT_SPAN}) + 헤더 없는 대조 1건(유저 ...002)")
