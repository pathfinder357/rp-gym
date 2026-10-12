#!/usr/bin/env python3
# 분해 결과 정리. 사용법: python3 breakdown.py <결과폴더> <N>
import sys, os, re, glob, collections

out, n = sys.argv[1], int(sys.argv[2])

def load(p):
    d = {}
    for line in open(p):
        parts = line.strip().split(',')
        if len(parts) == 3:
            d[(parts[0], parts[1])] = int(parts[2])
    return d

print(f"=== A. DB 통계 증감 (이벤트 1건당, N={n}) ===")
print("    샘플러·진행 관측 쿼리도 섞이지만 2초에 몇 건 수준이라 무시할 만하다")
b, a = load(f"{out}/pg_before.csv"), load(f"{out}/pg_after.csv")
rows = []
for k in sorted(set(a) | set(b)):
    dv = a.get(k, 0) - b.get(k, 0)
    if dv:
        rows.append((k[0], k[1], dv, dv / n))
for rel, col, dv, per in rows:
    print(f"    {rel:28s} {col:14s} {dv:>10d}   {per:8.2f} /건")

print()
print("=== B. 스레드 덤프 — 리스너 스레드가 무엇을 기다리고 있었나 ===")
def classify(frames):
    # 스택 전체를 보고 우선순위대로 판정한다. 맨 위 프레임만 보면
    # 오프셋 커밋 대기도 네트워크 poll 로 보여서 "일 없음" 으로 잘못 분류된다.
    st = "\n".join(frames)
    has = lambda pat: re.search(pat, st) is not None
    if has(r"HikariPool\.getConnection|ConcurrentBag\.borrow"):
        return "DB 커넥션 풀 대기"
    if has(r"org\.postgresql\."):
        if has(r"doCommit|commitTransaction|JdbcTransaction\.commit|\.commit\("):
            return "DB 커밋 대기"
        return "DB 쿼리 응답 대기"
    if has(r"commitSync|commitOffsetsSync|ConsumerCoordinator\.commit|commitIfNecessary|ackCurrent"):
        return "Kafka 오프셋 커밋"
    if has(r"jackson"):
        return "JSON 역직렬화"
    if has(r"org\.apache\.kafka\.clients\.consumer\..*poll|pollAndInvoke"):
        # 리스너를 부르는 중이 아니라 poll 안에 있다 = 가져올 것이 없어 기다리는 중
        if not has(r"onMessage|invokeListener|doInvokeRecordListener"):
            return "Kafka 폴링 (할 일 없음)"
    return "애플리케이션 코드 실행"

by_container = collections.defaultdict(collections.Counter)
dumps = sorted(glob.glob(f"{out}/threads/dump_*.txt"))
for f in dumps:
    text = open(f, errors="replace").read()
    for block in text.split("\n\n"):
        m = re.match(r'"([^"]+)"', block.strip())
        if not m:
            continue
        name = m.group(1)
        c = re.search(r"#(\d+)-(\d+)-C-1", name)
        if "KafkaListenerEndpointContainer" not in name and not c:
            continue
        container = f"리스너 #{c.group(1)}" if c else "리스너"
        frames = [l.strip() for l in block.splitlines() if l.strip().startswith("at ")]
        label = classify(frames)
        by_container[container][label] += 1
print(f"    덤프 {len(dumps)}회")
for cont, cnt in sorted(by_container.items()):
    total = sum(cnt.values())
    print(f"    [{cont}] 스레드×샘플 {total}")
    for lab, v in cnt.most_common():
        print(f"        {lab:22s} {v:5d}  {100*v/total:5.1f}%")

print()
print("=== C. DB 세션이 기다리던 것 (pg_stat_activity 샘플, game_db 의 active 세션) ===")
cnt = collections.Counter()
p = f"{out}/pg_activity.csv"
if os.path.exists(p):
    for line in open(p):
        parts = line.strip().split(',')
        if len(parts) >= 4:
            cnt[(parts[1], parts[2] or '-', parts[3] or '-')] += 1
total = sum(cnt.values()) or 1
for (state, wtype, wev), v in cnt.most_common(12):
    print(f"    {state:22s} {wtype:10s} {wev:22s} {v:5d}  {100*v/total:5.1f}%")
