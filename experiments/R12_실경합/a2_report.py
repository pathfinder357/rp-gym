"""A-2 반대 실험(naive-claim) 로그 분석 (2026-10-11)
사용: python a2_report.py <앱 로그>
센다: 파티별 '자바가 완료라고 판단한 횟수' (2 이상 = 중복 판정, 0 = 완료 판정 누락)
      유니크 위반(중복 키) 예외 · DLT 발행 흔적 · 완료 로그 줄 수
"""
import collections, re, sys

JUDGE = re.compile(r'\[A-2\] judge partyQuestId=(\S+) read=(\d+) delta=(\d+) target=(\d+) javaComplete=(true|false) dbClaimed=(\d)')
DONE = re.compile(r'PartyQuestProgressService.*rewardXp=')


def read_lines(path):
    raw = open(path, "rb").read()
    if raw[:2] in (b"\xff\xfe", b"\xfe\xff") or b"\x00" in raw[:200]:
        return raw.decode("utf-16", errors="replace").splitlines()
    return raw.decode("utf-8", errors="replace").splitlines()


lines = read_lines(sys.argv[1])
yes = collections.Counter()
mismatch = 0
judged = 0
for l in lines:
    m = JUDGE.search(l)
    if m:
        judged += 1
        pq, _, _, _, jc, db = m.groups()
        if jc == "true":
            yes[pq] += 1
        if (jc == "true") != (db == "1"):
            mismatch += 1
parties = {JUDGE.search(l).group(1) for l in lines if JUDGE.search(l)}
dup = sum(1 for p in parties if yes[p] >= 2)
none = sum(1 for p in parties if yes[p] == 0)
done_lines = sum(1 for l in lines if DONE.search(l))
dupkey = sum(1 for l in lines if "duplicate key" in l or "uk_outbox_events_aggregate" in l)
dlt = sum(1 for l in lines if ".DLT" in l)
print(f"== {sys.argv[1]}")
print(f"자바 판단 로그 {judged}줄 / 파티 {len(parties)}개")
print(f"자바가 '완료'라고 판단한 횟수 분포: {dict(sorted(collections.Counter(yes[p] for p in parties).items()))}")
print(f"  중복 판정(2번 이상) 파티: {dup}    <- 조건부 UPDATE 를 빼면 생기는 '중복 지급 시도'")
print(f"  완료 판정 0번 파티    : {none}    <- 누구도 완료라고 못 본 파티(보상 누락 후보)")
print(f"자바 판단 ≠ DB 선점 결과인 건수: {mismatch}")
print(f"완료 로그(rewardXp=) 줄 수: {done_lines}   (정상 코드는 파티 수와 같다)")
print(f"중복 키(유니크 제약) 흔적 줄 수: {dupkey} / DLT 흔적 줄 수: {dlt}")
