"""B-14 원인 추적 실험 — 데이터 생성기 (2026-10-12)

질문: DLT 알림이 Slack 에 온 뒤, 사람이 Loki 에서 eventId 로 "무엇이 왜 실패했는지" 를 찾을 수 있는가
  B  깨진 JSON 1건 (계약 위반 -> 재시도 없이 바로 DLT). 깨져 있지만 eventId 문자열은 들어 있다
  F  코드 버그 1건 (유저 b1400002 -> 실험 설정 fail-user-prefix 가 예외를 던짐 -> 1 · 2 · 4초 재시도 3번 뒤 DLT)
그다음 비교: 같은 원인 정보가 DLT 레코드 헤더(kafka_dlt-exception-*)에는 있는가
식별자 앞자리 b14: 유저 b1400001(B) · b1400002(F) / 이벤트 b1400009 (B = ...001, F = ...002)
"""
import os

here = os.path.dirname(os.path.abspath(__file__))
U = lambda pfx, n: f"{pfx}-0000-4000-8000-{n:012d}"
b_user, f_user = U("b1400001", 1), U("b1400002", 1)
b_line = f'{b_user}|{{"eventId":"{U("b1400009", 1)}","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"{b_user}", BROKEN'
f_line = (f'{f_user}|{{"eventId":"{U("b1400009", 2)}","eventType":"HEALTH_ACTIVITY_SYNCED","userId":"{f_user}",'
          f'"data":{{"activityDate":"__TODAY__","measuredAt":"__NOW__","cumulative":{{"steps":2000,"activeMinutes":0,"activeCalories":0}}}}}}')
with open(os.path.join(here, "02_inject.template.txt"), "w", encoding="utf-8", newline="\n") as f:
    f.write(b_line + "\n" + f_line + "\n")
cleanup = """-- B-14 데이터만 지운다
BEGIN;
DELETE FROM game_service.user_latest_snapshots WHERE user_id::text LIKE 'b140000%';
COMMIT;
"""
with open(os.path.join(here, "99_cleanup_b14.sql"), "w", encoding="utf-8", newline="\n") as f:
    f.write(cleanup)
print(f"주입 2건: 깨진 JSON(eventId {U('b1400009', 1)}) · 코드 버그(eventId {U('b1400009', 2)})")
