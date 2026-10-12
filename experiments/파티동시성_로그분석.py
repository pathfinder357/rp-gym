"""파티 동시성 실험(2026-09-22) 로그 분석.
사용: python 파티동시성_로그분석.py boot.log [boot-c1.log]
- boot.log 는 PowerShell Tee-Object 로 저장돼 UTF-16LE 이다.
- HealthEventConsumer 는 이벤트마다 2줄(개인 처리 완료, 파티 처리 완료)을 남긴다.
  그래서 스레드별 '줄 수'(232/296/272)는 이벤트 수의 2배다.
- 파티 트랜잭션 구간 = [개인 처리 완료 로그 시각, 파티 처리 완료 로그 시각] (ms 해상도, 근사)
"""
import re, sys, collections, datetime as dt, statistics
pat = re.compile(r'^(\S+)\s+\w+\s+\d+ --- \[game-service\] \[(ntainer#0-\d)-C-1\] '
                 r'\[[^,]*,[^,]*,55555555-0000-4000-8000-(\d+)\].*HealthEventConsumer.*result=(\S+)')
def run(fn):
    ev = collections.defaultdict(list)
    with open(fn, encoding='utf-16-le', errors='replace') as f:
        for l in f:
            m = pat.match(l.lstrip('﻿'))
            if m:
                ts, th, eid, res = m.groups()
                ev[eid].append((dt.datetime.fromisoformat(ts), th, res))
    lines = collections.Counter(r[1] for rows in ev.values() for r in rows)
    party = collections.defaultdict(list)
    for eid, rows in ev.items():
        rows.sort()
        party[int(eid) // 1000].append((rows[0][0], rows[1][0], rows[0][1], eid))
    dist = collections.Counter(len({r[2] for r in v}) for v in party.values())
    overlap = []
    for p, v in sorted(party.items()):
        hit = [(a[3], b[3]) for i, a in enumerate(v) for b in v[i+1:]
               if a[2] != b[2] and a[0] < b[1] and b[0] < a[1]]
        if hit: overlap.append((p, hit))
    d = [(r[1]-r[0]).total_seconds()*1000 for v in party.values() for r in v]
    print(f'== {fn}')
    print(f'이벤트 {len(ev)}건 / 파티 {len(party)}개')
    print('스레드별 로그 줄 수:', dict(sorted(lines.items())))
    print('스레드별 이벤트 수 :', dict(sorted(collections.Counter(r[0][1] for r in ev.values()).items())))
    print('파티별 처리 스레드 수 분포:', {f'{k}개': n for k, n in sorted(dist.items())})
    print(f'파티 트랜잭션 구간(ms) 중앙값 {statistics.median(d):.0f} / 최대 {max(d):.0f}')
    print(f'서로 다른 스레드의 파티 트랜잭션이 시간상 겹친 파티: {len(overlap)}개', overlap)
for fn in sys.argv[1:]: run(fn)
