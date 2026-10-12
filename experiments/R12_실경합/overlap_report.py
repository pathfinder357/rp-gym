"""R-12 실경합 실험 — "정말 동시에 처리됐나"를 로그로 센다 (2026-10-11)

사용: python overlap_report.py <앱 로그> [PostgreSQL 로그]
  - 앱 로그: bootRun 을 Tee-Object 로 저장한 파일 (UTF-16 이어도 된다. 인코딩은 알아서 맞춘다)
  - PostgreSQL 로그(선택): log_lock_waits 를 켜고 `docker logs` 로 저장한 파일

이벤트 한 건의 파티 트랜잭션 구간 = [개인 처리 완료 로그 시각, 파티 처리 완료 로그 시각] (ms 해상도 근사)
(HealthEventConsumer 가 개인 Tx -> 파티 Tx 순서로 처리하고, 각각 끝날 때 debug 로그를 한 줄씩 남긴다)

파티마다 센다
  최대 동시       : 같은 파티의 트랜잭션이 한 순간에 몇 개까지 동시에 열려 있었나 (파티션 3개면 최대 3)
  참여 스레드 전원 동시: 최대 동시 = 그 파티를 처리한 스레드 수
  완료 순간 겹침  : 완료시킨 이벤트의 구간이 다른 스레드의 구간과 겹침   <- 핵심 판정 기준
  완료 뒤 커밋     : 완료시킨 트랜잭션보다 늦게 커밋된 같은 파티 이벤트가 있음 (목표 < 인원 × 1000 일 때)
                    카운터 행 잠금은 커밋까지 쥐므로, 완료보다 늦게 커밋됐다면 카운터도 완료 뒤에 올린 것이다
                    = "완료 직후 도착한 기여"가 실제로 일어났다
"""
import collections
import datetime as dt
import re
import statistics
import sys

HEAD = r'^(\S+)\s+\w+\s+\d+ --- \[game-service\] \[(ntainer#\d+-\d+)-C-1\] \[[^,\]]*,[^,\]]*,55555555-0000-4000-8000-(\d+)\]'
EVENT = re.compile(HEAD + r'.*HealthEventConsumer.*result=(\S+)')
DONE = re.compile(HEAD + r'.*PartyQuestProgressService.*rewardXp=')


def read_lines(path):
    raw = open(path, "rb").read()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff") or b"\x00" in raw[:200]:
        text = raw.decode("utf-16", errors="replace")
    else:
        text = raw.decode("utf-8", errors="replace")
    return [l.lstrip("﻿").rstrip("\r\n") for l in text.splitlines()]


def ts(s):
    return dt.datetime.fromisoformat(s)


def app_report(path):
    ev = collections.defaultdict(list)
    completed = {}
    for line in read_lines(path):
        m = EVENT.match(line)
        if m:
            t, th, eid, _ = m.groups()
            ev[eid].append((ts(t), th))
            continue
        m = DONE.match(line)
        if m:
            t, th, eid = m.groups()
            completed[int(eid) // 1000] = eid

    parties = collections.defaultdict(list)  # party -> [(start, end, thread, eid)]
    broken = 0
    for eid, rows in ev.items():
        rows.sort()
        if len(rows) < 2:
            broken += 1
            continue
        parties[int(eid) // 1000].append((rows[0][0], rows[-1][0], rows[0][1], eid))

    dist = collections.Counter()
    maxc_dist = collections.Counter()
    allthreads = comp = late = 0
    durations = []
    no_comp = []
    for p, v in sorted(parties.items()):
        threads = {r[2] for r in v}
        dist[len(threads)] += 1
        durations += [(r[1] - r[0]).total_seconds() * 1000 for r in v]
        maxc = max(sum(1 for r in v if r[0] <= x[0] < r[1]) for x in v)
        maxc_dist[maxc] += 1
        if maxc == len(threads) and len(threads) > 1:
            allthreads += 1
        ce = completed.get(p)
        c = next((r for r in v if r[3] == ce), None)
        if c:
            if any(o[2] != c[2] and o[0] < c[1] and c[0] < o[1] for o in v if o is not c):
                comp += 1
            else:
                no_comp.append(p)
            if any(o[1] > c[1] for o in v if o is not c):
                late += 1

    n = len(parties)
    print(f"== {path}")
    print(f"이벤트 {len(ev)}건 / 파티 {n}개 / 완료 로그 {len(completed)}줄" + (f" / 로그 짝이 안 맞는 이벤트 {broken}건" if broken else ""))
    print("스레드별 이벤트 수:", dict(sorted(collections.Counter(r[2] for v in parties.values() for r in v).items())))
    print("파티별 처리 스레드 수:", {f"{k}개": c for k, c in sorted(dist.items())})
    print("파티별 최대 동시 트랜잭션 수:", {f"{k}개": c for k, c in sorted(maxc_dist.items())})
    if durations:
        print(f"파티 트랜잭션 구간(ms): 중앙값 {statistics.median(durations):.0f} / 최대 {max(durations):.0f}")
    print(f"참여 스레드 전원 동시: {allthreads}/{n}")
    print(f"완료 순간 겹침       : {comp}/{n}   <- 판정 기준: 과반이어야 '완료를 두고 실제로 경합했다'고 말할 수 있다")
    print(f"완료 뒤 커밋된 기여  : {late}/{n}   (4명 · 목표 3000이면 100에 가까워야 한다)")
    if no_comp:
        print("완료 순간 겹치지 않은 파티(앞 20개):", no_comp[:20])


def pg_report(path):
    lines = read_lines(path)
    waits = on_party = 0
    for i, line in enumerate(lines):
        if "still waiting for" in line:
            waits += 1
            if any('relation "party_quests"' in l for l in lines[i + 1:i + 5]):
                on_party += 1
    dead = sum("deadlock detected" in l for l in lines)
    print(f"== {path}")
    print(f"행 잠금 대기 로그: {waits}건 (그중 party_quests 행: {on_party}건) / 데드락: {dead}건")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    app_report(sys.argv[1])
    if len(sys.argv) > 2:
        pg_report(sys.argv[2])
