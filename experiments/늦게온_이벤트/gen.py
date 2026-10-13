"""늦게 온 이벤트 실험 — 데이터 생성기 (2026-10-11)

막은 경로 셋을 실제 Kafka 파이프라인으로 확인한다. (파티 기준값 날짜 대조 파티 생성 경로는 API 라 이 실험 밖, 단위 테스트로만)
  P  개인 퀘스트(개인 퀘스트 워터마크) : 기준값 3000 · 목표 2000. 어제 밤(누적 8000) 이벤트가 먼저 늦게 도착한다.
       P-A 25명: 어제 이벤트만 → ACTIVE 그대로, 워터마크 그대로(NULL), 보상 0      (고치기 전이면 8000-3000=5000 으로 공짜 완료)
       P-B 25명: 어제 이벤트 뒤 오늘 이벤트(누적 6000) → 오늘 이벤트로만 완료, last_cumulative_val = 6000 (8000 이면 어제 값으로 완료된 것)
  Q  파티 기여(파티 기여 날짜 대조)    : 파티 25개 × 4명, 기준 1000 · 목표 4000. 어제 이벤트(누적 8000)가 먼저, 그다음 오늘 이벤트(누적 2000)
       → 어제 이벤트는 OTHER_DAY 로 무시, 오늘 이벤트로 기여 1000씩 · 카운터 4000 · 완료 25 (고치기 전이면 7000씩 들어가 첫 이벤트로 공짜 완료)
  S  제안(만료 제안 저장 차단)        : 근거 시각 31분 전 10건 → 저장 0 · 카드(Outbox) 0 / 29분 전 10건(대조) → 저장 10 · 카드 10

식별자 앞자리 (99_cleanup 과 겹치지 않게 a5 로 시작): 개인 유저 a5000001 · 개인 퀘스트 a5000002 · 파티 a5000003 ·
  파티 퀘스트 a5000004 · 파티 유저 a5000005 · 파티 멤버 행 a5000006 · 제안 유저 a5000007 · 제안 a5000008 · 이벤트 a5000009
출력: 01_seed.sql, 02_events.template.txt, 03_verify.sql, 99_cleanup.sql
"""
import os

here = os.path.dirname(os.path.abspath(__file__))
U = lambda pfx, n: f"{pfx}-0000-4000-8000-{n:012d}"
TODAY_END = "((now() AT TIME ZONE 'Asia/Seoul')::date + time '23:59:59') AT TIME ZONE 'Asia/Seoul'"

def ev(eid, user, etype, data):
    return f'{user}|{{"eventId":"{U("a5000009", eid)}","eventType":"{etype}","userId":"{user}","data":{data}}}'

def sync(day, at, steps):
    return f'{{"activityDate":"{day}","measuredAt":"{at}","cumulative":{{"steps":{steps},"activeMinutes":0,"activeCalories":0}}}}'

seed = [f"""-- 늦게 온 이벤트 시드 (gen.py 가 만듦)
DO $$ BEGIN
  IF (now() AT TIME ZONE 'Asia/Seoul')::time > time '23:30' OR (now() AT TIME ZONE 'Asia/Seoul')::time < time '00:10' THEN
    RAISE EXCEPTION '자정 근처다. 어제/오늘 날짜가 갈리지 않게 00:10 ~ 23:30 사이에 하자';
  END IF;
END $$;
BEGIN;
-- P: 개인 퀘스트 50개 (1~25 = P-A, 26~50 = P-B)
INSERT INTO game_service.quests (quest_id, user_id, suggestion_id, title, metric, target_val, baseline_val,
    baseline_measured_at, last_applied_measured_at, last_cumulative_val, status, reward_xp, expired_at, version)
SELECT ('a5000002-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('a5000001-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       gen_random_uuid(), 'A5 개인 ' || i, 'STEPS', 2000, 3000,
       now() - interval '1 minute', NULL, NULL, 'ACTIVE', 50, {TODAY_END}, 0
  FROM generate_series(1, 50) AS i;
-- Q: 파티 25개 × 4명
INSERT INTO game_service.parties (id, party_name, owner_id, status, visibility, metric, max_member, current_member,
    matching_deadline_at, ends_at)
SELECT ('a5000003-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid, 'A5 파티 ' || i,
       ('a5000005-0000-4000-8000-' || lpad((i * 10 + 1)::text, 12, '0'))::uuid,
       'ACTIVE', 'PRIVATE', 'STEPS', 4, 4, now() - interval '1 hour', now() + interval '6 days'
  FROM generate_series(1, 25) AS i;
INSERT INTO game_service.party_quests (party_quest_id, party_id, title, metric, target_val, current_val, status,
    reward_xp, started_at, expired_at)
SELECT ('a5000004-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('a5000003-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       'A5 파티 퀘스트 ' || i, 'STEPS', 4000, 0, 'ACTIVE', 30, now() - interval '1 minute', {TODAY_END}
  FROM generate_series(1, 25) AS i;
INSERT INTO game_service.party_quest_members (party_quest_member_id, party_quest_id, user_id, baseline_val,
    contributed_val, last_applied_measured_at)
SELECT ('a5000006-0000-4000-8000-' || lpad((i * 10 + j)::text, 12, '0'))::uuid,
       ('a5000004-0000-4000-8000-' || lpad(i::text, 12, '0'))::uuid,
       ('a5000005-0000-4000-8000-' || lpad((i * 10 + j)::text, 12, '0'))::uuid, 1000, 0, NULL
  FROM generate_series(1, 25) AS i, generate_series(1, 4) AS j;
COMMIT;
SELECT (SELECT count(*) FROM game_service.quests WHERE quest_id::text LIKE 'a5000002%') AS personal_quests,
       (SELECT count(*) FROM game_service.party_quests WHERE party_quest_id::text LIKE 'a5000004%') AS party_quests,
       (SELECT count(*) FROM game_service.party_quest_members WHERE party_quest_id::text LIKE 'a5000004%') AS party_members;
-- 50 / 25 / 100
"""]

lines, n = [], 0
# 1) 늦게 온 어제 이벤트가 먼저 (P 전원, Q 전원)
for i in range(1, 51):
    n += 1; lines.append(ev(n, U("a5000001", i), "HEALTH_ACTIVITY_SYNCED", sync("__YESTERDAY__", "__YESTERDAY_2300__", 8000)))
for i in range(1, 26):
    for j in range(1, 5):
        n += 1; lines.append(ev(n, U("a5000005", i * 10 + j), "HEALTH_ACTIVITY_SYNCED", sync("__YESTERDAY__", "__YESTERDAY_2300__", 8000)))
# 2) 오늘 이벤트 (P-B 만, Q 전원)
for i in range(26, 51):
    n += 1; lines.append(ev(n, U("a5000001", i), "HEALTH_ACTIVITY_SYNCED", sync("__TODAY__", "__NOW__", 6000)))
for i in range(1, 26):
    for j in range(1, 5):
        n += 1; lines.append(ev(n, U("a5000005", i * 10 + j), "HEALTH_ACTIVITY_SYNCED", sync("__TODAY__", "__NOW__", 2000)))
# 3) 제안: 1~10 = 31분 전(만료 도착), 11~20 = 29분 전(대조)
for i in range(1, 21):
    based = "__NOW_MINUS_31__" if i <= 10 else "__NOW_MINUS_29__"
    data = (f'{{"suggestionId":"{U("a5000008", i)}","activityDate":"__TODAY__","basedOnMeasuredAt":"{based}",'
            f'"title":"A5 suggestion {i}","metric":"STEPS","targetValue":3000}}')
    n += 1; lines.append(ev(n, U("a5000007", i), "QUEST_SUGGESTED", data))

verify = """-- 늦게 온 이벤트 검증 (기대값은 각 제목 괄호 안)
\\echo '=== P-A. yesterday-only personal quests (expect ACTIVE 25, watermark NULL 25, reward 0) ==='
SELECT count(*) FILTER (WHERE status = 'ACTIVE') AS active,
       count(*) FILTER (WHERE last_applied_measured_at IS NULL) AS watermark_null,
       (SELECT count(*) FROM game_service.xp_ledgers l WHERE l.source_type = 'QUEST'
           AND l.source_id IN (SELECT quest_id FROM game_service.quests WHERE quest_id::text LIKE 'a5000002%'
                                AND user_id::text <= 'a5000001-0000-4000-8000-000000000025')) AS rewards
  FROM game_service.quests
 WHERE quest_id::text LIKE 'a5000002%' AND user_id::text <= 'a5000001-0000-4000-8000-000000000025';

\\echo '=== P-B. yesterday then today (expect COMPLETED 25, last_cumulative 6000 x25, 8000 x0, reward 25) ==='
SELECT count(*) FILTER (WHERE status = 'COMPLETED') AS completed,
       count(*) FILTER (WHERE last_cumulative_val = 6000) AS cum_6000,
       count(*) FILTER (WHERE last_cumulative_val = 8000) AS cum_8000,
       (SELECT count(*) FROM game_service.xp_ledgers l WHERE l.source_type = 'QUEST'
           AND l.source_id IN (SELECT quest_id FROM game_service.quests WHERE quest_id::text LIKE 'a5000002%'
                                AND user_id::text > 'a5000001-0000-4000-8000-000000000025')) AS rewards
  FROM game_service.quests
 WHERE quest_id::text LIKE 'a5000002%' AND user_id::text > 'a5000001-0000-4000-8000-000000000025';

\\echo '=== Q. party (expect COMPLETED 25, counter 4000 x25, contribution 1000 x100, reward 100) ==='
SELECT (SELECT count(*) FROM game_service.party_quests WHERE party_quest_id::text LIKE 'a5000004%' AND status = 'COMPLETED') AS completed,
       (SELECT count(*) FROM game_service.party_quests WHERE party_quest_id::text LIKE 'a5000004%' AND current_val = 4000) AS counter_4000,
       (SELECT count(*) FROM game_service.party_quest_members WHERE party_quest_id::text LIKE 'a5000004%' AND contributed_val = 1000) AS contrib_1000,
       (SELECT count(*) FROM game_service.party_quest_members WHERE party_quest_id::text LIKE 'a5000004%' AND contributed_val > 1000) AS contrib_over,
       (SELECT count(*) FROM game_service.xp_ledgers WHERE source_type = 'PARTY_QUEST' AND source_id::text LIKE 'a5000004%') AS rewards;

\\echo '=== S. suggestions (expect expired_31m stored 0 / cards 0, control_29m stored 10 / cards 10) ==='
SELECT (SELECT count(*) FROM game_service.quest_suggestions WHERE suggestion_id::text LIKE 'a5000008%'
           AND suggestion_id::text <= 'a5000008-0000-4000-8000-000000000010') AS expired_31m_stored,
       (SELECT count(*) FROM game_service.outbox_events WHERE event_type = 'QUEST_SUGGESTED' AND aggregate_id::text LIKE 'a5000008%'
           AND aggregate_id::text <= 'a5000008-0000-4000-8000-000000000010') AS expired_31m_cards,
       (SELECT count(*) FROM game_service.quest_suggestions WHERE suggestion_id::text LIKE 'a5000008%'
           AND suggestion_id::text > 'a5000008-0000-4000-8000-000000000010') AS control_29m_stored,
       (SELECT count(*) FROM game_service.outbox_events WHERE event_type = 'QUEST_SUGGESTED' AND aggregate_id::text LIKE 'a5000008%'
           AND aggregate_id::text > 'a5000008-0000-4000-8000-000000000010') AS control_29m_cards;
"""

cleanup = """-- 늦게 온 이벤트 데이터만 지운다 (a5 로 시작하는 식별자)
BEGIN;
DELETE FROM game_service.outbox_events WHERE aggregate_id::text LIKE 'a500000%';
DELETE FROM game_service.xp_ledgers WHERE user_id::text LIKE 'a500000%';
DELETE FROM game_service.wallets WHERE user_id::text LIKE 'a500000%';
DELETE FROM game_service.quest_suggestions WHERE suggestion_id::text LIKE 'a5000008%';
DELETE FROM game_service.quests WHERE quest_id::text LIKE 'a5000002%';
DELETE FROM game_service.party_quest_members WHERE party_quest_id::text LIKE 'a5000004%';
DELETE FROM game_service.party_quests WHERE party_quest_id::text LIKE 'a5000004%';
DELETE FROM game_service.parties WHERE id::text LIKE 'a5000003%';
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'a500000%';
DELETE FROM game_service.characters WHERE user_id::text LIKE 'a500000%';
COMMIT;
"""
for name, body in [("01_seed.sql", seed[0]), ("02_events.template.txt", "\n".join(lines) + "\n"),
                   ("03_verify.sql", verify), ("99_cleanup.sql", cleanup)]:
    with open(os.path.join(here, name), "w", encoding="utf-8", newline="\n") as f:
        f.write(body)
print(f"events {len(lines)} / files written")
