"""로컬 관측 실험용 알림 규칙(alert-rules-observe.yml)을 원본에서 다시 만든다 (2026-10-12)

원본 provisioning/alerting/alert-rules.yml 에서 game-consumer-alerts 그룹만 남기고,
나머지 그룹의 규칙은 deleteRules 로 지운다(이미 로컬 Grafana 에 올라간 것까지).
사용: python monitoring/grafana/make_alert_rules_observe.py   (저장소 루트에서, PyYAML 필요)
"""
import os
import yaml

here = os.path.dirname(os.path.abspath(__file__))
src = os.path.join(here, "provisioning", "alerting", "alert-rules.yml")
dst = os.path.join(here, "alert-rules-observe.yml")
KEEP = "game-consumer-alerts"

d = yaml.safe_load(open(src, encoding="utf-8"))
keep = [g for g in d["groups"] if g["name"] == KEEP]
removed = [r["uid"] for g in d["groups"] if g["name"] != KEEP for r in g["rules"]]
assert len(keep) == 1, f"{KEEP} 그룹을 찾지 못함"
out = {"apiVersion": d.get("apiVersion", 1), "groups": keep,
       "deleteRules": [{"orgId": 1, "uid": u} for u in removed]}
head = ("# ============================================================\n"
        "# 로컬 관측 실험 전용 알림 규칙 — 손으로 고치지 말 것 (make_alert_rules_observe.py 가 만듦)\n"
        f"# 원본 provisioning/alerting/alert-rules.yml 에서 {KEEP} 그룹만 남긴 사본이다.\n"
        "# 로컬에서는 Health 서비스 · CPU 규칙이 데이터 없음 등으로 Slack 을 어지럽혀서 뺐다(deleteRules 로 이미 올라간 것도 지운다).\n"
        "# docker-compose-observe.yml 이 이 파일을 Grafana 안의 alert-rules.yml 자리에 덮어 붙인다. 운영(prod)은 원본을 그대로 쓴다.\n"
        "# 원본의 game 규칙을 고쳤으면: python monitoring/grafana/make_alert_rules_observe.py\n"
        f"# 뺀 규칙: {', '.join(removed)}\n"
        "# ============================================================\n")
with open(dst, "w", encoding="utf-8", newline="\n") as f:
    f.write(head + yaml.safe_dump(out, allow_unicode=True, sort_keys=False, width=200))
print(f"만듦: {dst} (규칙 {len(keep[0]['rules'])}개, 지울 규칙 {removed})")
