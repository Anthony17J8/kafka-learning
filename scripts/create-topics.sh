#!/usr/bin/env bash
# Создаёт учебные топики. Подстраивается под single/cluster.
#   ./scripts/create-topics.sh single
#   ./scripts/create-topics.sh cluster
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-single}"

if [ "$MODE" = "single" ]; then
  export KAFKA_CONTAINER=kafka
  export BOOTSTRAP=localhost:9092
  RF=1; MIN_ISR=1
else
  export KAFKA_CONTAINER=kafka-1
  export BOOTSTRAP=kafka-1:19093
  RF=3; MIN_ISR=2
fi

create() {
  local topic="$1"; local parts="$2"; shift 2
  echo ">> create topic: $topic (partitions=$parts, rf=$RF)"
  ./scripts/kafka.sh kafka-topics.sh --create --if-not-exists \
    --topic "$topic" --partitions "$parts" --replication-factor "$RF" "$@"
}

create demo-events 3
create orders 3 --config min.insync.replicas=$MIN_ISR
create orders-enriched 3
create users 3 --config cleanup.policy=compact
create metrics 1
create dlq 1

echo
./scripts/kafka.sh kafka-topics.sh --list
