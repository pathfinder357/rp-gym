#!/usr/bin/env bash
# 사용법:  ./run_spike.sh <라벨> <시나리오> <N>
#   예)    ./run_spike.sh S0-N1000 S0 1000
# 결과는 results/<라벨>/ 에 쌓인다.
set -euo pipefail
LABEL=$1; SCEN=$2; N=$3
WARM_N=${WARM_N:-500}
DIR=$(cd "$(dirname "$0")" && pwd)
OUT=$DIR/results/$LABEL; mkdir -p "$OUT"
cd "$DIR"

MEAS=66666666-0000-4000-8000-      # 측정 유저
WARM=66666666-0000-4000-9000-      # 워밍업 유저
PSQL="docker exec -i rp-gym-postgres-game psql -U postgres -d game_db -v ON_ERROR_STOP=1"
GAME=$(docker ps --format '{{.Names}}' | grep -E '^rp-gym-game-(blue|green)$' | head -1)
# 어떤 이유로 스크립트가 끝나든(Ctrl+C 포함) game 이 얼어 있는 채로 남지 않게 한다
trap 'docker unpause "$GAME" > /dev/null 2>&1 || true; kill $(jobs -p) 2>/dev/null || true' EXIT

# kafka-consumer-groups.sh 는 호출할 때마다 JVM 을 띄운다. 2코어 서버에서 매초 부르면 측정을 오염시키므로
# 시작 전과 끝난 뒤에만 부른다. 진행 중 관측은 DB 에서 한다.
lag() {
  docker exec rp-gym-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --describe --group game-service 2>/dev/null | awk '$2=="health.events" && $6!="-" {s+=$6} END{print s+0}'
}
# 반복문 몸통 마지막에 "[ 조건 ] && ..." 를 두면, 조건이 거짓일 때 그 1 이 함수의 종료 코드가 되어
# set -e 가 스크립트를 조용히 끝낸다. 2026-09-28 S2 워밍업 직후 멈춘 원인. 그래서 if 로 쓴다.
wait_lag0() {
  local t=0
  while [ "$(lag)" != "0" ]; do
    sleep 3; t=$((t+3))
    if [ $t -gt 3600 ]; then echo "lag 가 1시간 안에 0이 안 됨"; exit 1; fi
  done
  return 0
}

jmeter() {   # $1=prefix $2=n $3=measuredAt $4=activityDate $5=tag $6=cpus
  docker run --rm --name spike-jmeter --network rp-gym-shared-network \
    --user "$(id -u):$(id -g)" -e HOME=/tmp \
    --cpus "$6" --memory 768m -e HEAP="-Xms256m -Xmx512m" \
    -v "$DIR":/work -w /work eclipse-temurin:17-jre \
    /work/apache-jmeter-5.6.3/bin/jmeter -n -t /work/sync_spike.jmx \
      -Jbootstrap=kafka:9092 -Jn="$2" -Jprefix="$1" -JmeasuredAt="$3" -JactivityDate="$4" \
      -JsendTimesFile="/work/results/$LABEL/send_$5.txt" \
      -l "/work/results/$LABEL/jmeter_$5.jtl" -j "/work/results/$LABEL/jmeter_$5.log" > "$OUT/jmeter_$5.out"
}

echo "[0] 사전 점검  game=$GAME"
[ -n "$GAME" ] || { echo "game-service 컨테이너가 없다"; exit 1; }
echo "    현재 lag=$(lag)"; wait_lag0

echo "[1] 정리";        $PSQL -q < 99_cleanup.sql
echo "[2] 시드 $SCEN N=$N"; $PSQL -v n="$N" -v prefix="$MEAS" < "01_seed_$SCEN.sql"

echo "[3] 워밍업 $WARM_N 건"
WT=$(TZ=Asia/Seoul date -d '-1 minute' +%Y-%m-%dT%H:%M:%S+09:00)
jmeter "$WARM" "$WARM_N" "$WT" "${WT:0:10}" warmup 0.5
wait_lag0

echo "[4] 주입 $N 건 (관측 시작)"
T=$(TZ=Asia/Seoul date +%Y-%m-%dT%H:%M:%S+09:00)
echo "measuredAt=$T" > "$OUT/meta.txt"
( while :; do
    docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' 2>/dev/null \
      | grep -E "^($GAME|rp-gym-postgres-game|rp-gym-kafka|spike-jmeter)," | sed "s/^/$(date +%s),/"
    sleep 5
  done ) > "$OUT/stats.csv" &
STATS=$!
( echo "epoch_s,processed"; while :; do
    c=$($PSQL -At -c "SELECT count(*) FROM game_service.user_latest_snapshots WHERE user_id BETWEEN '${MEAS}000000000000' AND '${MEAS}ffffffffffff' AND measured_at = '$T'")
    echo "$(date +%s),$c"; [ "$c" -ge "$N" ] && break; sleep 2
  done ) > "$OUT/progress.csv" &
PROG=$!

# 컨슈머를 얼린 채로 N 건을 전부 쌓는다.
# 첫 회차(2026-09-28)에서 발행 5.2초 / 소진 5.4초로, 컨슈머가 아니라 부하 생성기의 속도를 쟀다.
# 얼려두면 발행 속도와 소진 속도가 완전히 분리되고, "정각에 N 건이 이미 쌓여 있다" 는 최악의 순간이 그대로 재현된다.
# 얼어 있는 동안 game 은 CPU 를 안 쓰므로 JMeter 에 1.5코어를 줘도 측정이 오염되지 않는다.
# 컨슈머 세션 타임아웃(기본 45초) 안에 풀어야 리밸런싱이 안 섞인다.
# 분해 모드 (BREAKDOWN=1): 측정용이 아니다. 스레드 덤프와 샘플링이 처리량을 깎으므로 숫자는 기준선과 비교하지 않는다
if [ "${BREAKDOWN:-0}" = "1" ]; then
  $PSQL -At -F, < pg_snapshot.sql > "$OUT/pg_before.csv"
  mkdir -p "$OUT/threads"
fi
docker pause "$GAME" > /dev/null
PAUSED_AT=$(date +%s)
jmeter "$MEAS" "$N" "$T" "${T:0:10}" main 1.5 || { docker unpause "$GAME"; exit 1; }
FROZEN=$(( $(date +%s) - PAUSED_AT ))
drain_start_ms=$(date +%s%3N)
docker unpause "$GAME" > /dev/null
echo "    발행 완료 (얼린 시간 ${FROZEN}초), 소진 대기"
if [ "${BREAKDOWN:-0}" = "1" ]; then
  ( i=0; sleep 2
    while kill -0 $PROG 2>/dev/null; do
      i=$((i+1))
      docker exec "$GAME" jcmd 1 Thread.print > "$OUT/threads/dump_$(printf %03d $i).txt" 2>/dev/null || true
      $PSQL -At -F, -c "SELECT now(), state, wait_event_type, wait_event FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state <> 'idle'" >> "$OUT/pg_activity.csv" || true
      sleep 2
    done ) &
  SAMPLER=$!
fi
[ "$FROZEN" -ge 40 ] && echo "    ⚠ 40초 이상 얼렸다. 리밸런싱이 섞였을 수 있다 — 결과를 쓰기 전에 game 로그 확인"
wait $PROG
kill $STATS 2>/dev/null || true
if [ "${BREAKDOWN:-0}" = "1" ]; then
  wait $SAMPLER 2>/dev/null || true
  # PostgreSQL 은 세션별 통계를 최대 10초 정도 모았다가 공유 통계에 반영한다. 기다리지 않으면 뒤쪽 증감이 빠진다
  sleep 12
  $PSQL -At -F, < pg_snapshot.sql > "$OUT/pg_after.csv"
fi
echo "    소진 후 lag=$(lag)"

. "$OUT/send_main.txt"
echo "[5] 결과"
$PSQL -v lo="${MEAS}000000000000" -v hi="${MEAS}ffffffffffff" -v t="$T" -v n="$N" \
      -v send_start_ms="$send_start_ms" -v send_end_ms="$send_end_ms" -v drain_start_ms="$drain_start_ms" < 03_result.sql | tee "$OUT/result.txt"

echo "[6] 환경 기록"
{ echo "label=$LABEL scenario=$SCEN n=$N warm_n=$WARM_N measuredAt=$T game=$GAME frozen_sec=$FROZEN"
  nproc; free -h
  docker exec rp-gym-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic health.events | head -1
  docker exec rp-gym-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group game-service --members 2>/dev/null
  docker inspect "$GAME" --format 'image={{.Config.Image}} started={{.State.StartedAt}}'
} > "$OUT/env.txt" 2>&1
if [ "${BREAKDOWN:-0}" = "1" ]; then
  echo "[7] 분해"
  python3 breakdown.py "$OUT" "$N" | tee "$OUT/breakdown.txt"
fi
echo "완료 → $OUT"
