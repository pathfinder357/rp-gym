#!/usr/bin/env bash
# 한 번만 실행한다. JMeter 를 받고, Kafka 컨테이너에서 클라이언트 jar 를 꺼내 JMeter lib 에 넣는다.
set -euo pipefail
DIR=$(cd "$(dirname "$0")" && pwd)
cd "$DIR"
JM=apache-jmeter-5.6.3

if [ ! -d "$JM" ]; then
  curl -fsSLO "https://archive.apache.org/dist/jmeter/binaries/$JM.tgz"
  tar xzf "$JM.tgz" && rm "$JM.tgz"
fi

# 브로커와 같은 버전의 클라이언트를 쓴다. 압축 라이브러리는 안 쓰지만 클래스 로딩 실패를 피하려고 같이 넣는다
for j in $(docker exec rp-gym-kafka sh -c 'ls /opt/kafka/libs/ | grep -E "^(kafka-clients|lz4|snappy|zstd)"'); do
  docker cp "rp-gym-kafka:/opt/kafka/libs/$j" "$JM/lib/"
done
ls "$JM/lib" | grep -E "kafka-clients|lz4|snappy|zstd"

# JMeter 를 돌릴 런타임 이미지. game-service 와 같은 계열(temurin 17)
docker pull -q eclipse-temurin:17-jre
chmod +x run_spike.sh
echo "setup 완료"
