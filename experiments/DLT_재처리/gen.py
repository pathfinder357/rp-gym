"""DLT 재처리 DLT 재처리 실험 — 데이터 생성기 (2026-10-11)

DLT 재처리 재처리가 두 가지를 하는지 실제 Kafka 로 본다.
  F  버그로 DLT 에 간 이벤트 10건: 1단계에서 실험 설정(fail-user-prefix)이 일부러 예외를 던져 재시도 3번 뒤 DLT.
     설정을 빼고 다시 띄운 뒤(= 버그 수정 배포) 재처리 -> 개인 퀘스트 완료 10 · 보상 10건
  재처리해도 보상이 다시 나가면 안 되는 것 (1단계 뒤 DLT 에 직접 넣는다)
     D  이미 처리된 이벤트 10건(같은 eventId): 1단계에서 정상 완료 · 보상 -> 재처리해도 보상 그대로 10건
     Y  어제 이벤트 10건(어제 23:00, 누적 8000): 오늘 퀘스트(기준 1000 · 목표 500) -> 재처리해도 ACTIVE · 보상 0
     S  근거 시각 31분 지난 제안 10건 -> 재처리해도 저장 0 · 카드(Outbox) 0
  B  깨진 JSON 1건: 1단계에서 계약 위반으로 바로 DLT. 재처리할 때마다 다시 DLT 로 돌아오고,
     x-replay-count 가 상한(3)에 닿으면 더 되돌리지 않는다(DLT <-> 원본 무한 순환 방지)

식별자 앞자리 a6: 유저 a6000001 (F 101~110 · D 201~210 · Y 301~310 · B 999) / 개인 퀘스트 a6000002 /
  제안 유저 a6000007 · 제안 a6000008 / 이벤트 a6000009
출력: 01_seed.sql, 02_phase1.template.txt(health.events), 03_dlt_direct.template.txt(health.events.DLT),
      04_verify.sql, 99_cleanup.sql
"""
import os

here = os.path.dirname(os.path.abspath(__file__))
U = lambda pfx, n: f"{pfx}-0000-4000-8000-{n:012d}"
TODAY_END = "((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'"
F, D, Y = range(101, 111), range(201, 211), range(301, 311)


def ev(eid, user, etype, data):
    return f'{user}|{{"eventId":"{U("a6000009", eid)}","eventType":"{etype}","userId":"{user}","data":{data}}}'


def sync(day, at, steps):
    return f'{{"activityDate":"{day}","measuredAt":"{at}","cumulative":{{"steps":{steps},"activeMinutes":0,"activeCalories":0}}}}'


users = list(F) + list(D) + list(Y)
seed = f"""-- DLT 재처리 시드 (gen.py 가 만듦)
DO $$ BEGIN
  IF (now() AT TIME ZONE 'Asia/Seoul')::time > time '23:30' OR (now() AT TIME ZONE 'Asia/Seoul')::time < time '00:10' THEN
    RAISE EXCEPTION '자정 근처다. 어제/오늘 날짜가 갈리지 않게 00:10 ~ 23:30 사이에 하자';
  END IF;
END $$;
BEGIN;
-- F · D · Y: 개인 퀘스트 30개 (기준 1000 · 목표 500 · 보상 10). 오늘 이벤트(누적 2000)면 완료
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
SELECT ('a6000002-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('a6000001-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       gen_random_uuid(), 'A6 personal ' || i, 'STEPS', 500, 1000,
       now() - interval '1 minute', NULL, NULL, 'ACTIVE', 10, {TODAY_END}, 0
  FROM unnest(ARRAY[{",".join(str(u) for u in users)}]) AS i;
COMMIT;
SELECT count(*) AS a6_quests FROM game_service.quests WHERE quest_id::text LIKE 'a6000002%';
-- 30
"""

n = 0
phase1 = []
for i in list(F) + list(D):
    n += 1
    phase1.append(ev(i, U("a6000001", i), "HEALTH_ACTIVITY_SYNCED", sync("__TODAY__", "__NOW__", 2000)))
phase1.append(f'{U("a6000001", 999)}|{{"eventId": broken json for DLT 재처리')

direct = []
for i in D:   # 1단계와 똑같은 레코드(같은 eventId)
    direct.append(ev(i, U("a6000001", i), "HEALTH_ACTIVITY_SYNCED", sync("__TODAY__", "__NOW__", 2000)))
for i in Y:
    direct.append(ev(i, U("a6000001", i), "HEALTH_ACTIVITY_SYNCED", sync("__YESTERDAY__", "__YESTERDAY_2300__", 8000)))
for i in range(1, 11):
    data = (f'{{"suggestionId":"{U("a6000008", i)}","activityDate":"__TODAY__","basedOnMeasuredAt":"__NOW_MINUS_31__",'
            f'"title":"A6 suggestion {i}","metric":"STEPS","targetValue":3000}}')
    direct.append(ev(500 + i, U("a6000007", i), "QUEST_SUGGESTED", data))

verify = r"""-- DLT 재처리 검증 (기대값은 각 제목 괄호 안)
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
"""

cleanup = """-- DLT 재처리 데이터만 지운다 (a6 로 시작하는 식별자)
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a600000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a600000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'a600000%';
DELETE FROM game_service.quest_suggestions WHERE suggestion_id::text LIKE 'a6000008%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'a6000002%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'a600000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'a600000%';
COMMIT;
"""

for name, body in [("01_seed.sql", seed), ("02_phase1.template.txt", "\n".join(phase1) + "\n"),
                   ("03_dlt_direct.template.txt", "\n".join(direct) + "\n"), ("04_verify.sql", verify),
                   ("99_cleanup.sql", cleanup)]:
    with open(os.path.join(here, name), "w", encoding="utf-8", newline="\n") as f:
        f.write(body)
print(f"1단계(health.events) {len(phase1)}건: F 10 + D 10 + 깨진 JSON 1 / DLT 직접 {len(direct)}건: D 10 + Y 10 + S 10")
print("DLT 기대: 1단계 뒤 F 10 + B 1 = 11건 쌓임, 직접 넣은 30건까지 41건 -> 첫 재처리 41건 되돌림")
print("만든 파일: 01_seed.sql, 02_phase1.template.txt, 03_dlt_direct.template.txt, 04_verify.sql, 99_cleanup.sql")
