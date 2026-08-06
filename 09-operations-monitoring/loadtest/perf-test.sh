#!/usr/bin/env bash
# ЭТАП 9, задача 3: нагрузочное тестирование штатными утилитами Kafka.
set -euo pipefail
cd "$(dirname "$0")/../.."

export KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka-1}"
BOOT="${BOOTSTRAP:-kafka-1:19093}"
TOPIC="${TOPIC:-perf-test}"
RECORDS="${RECORDS:-1000000}"
SIZE="${SIZE:-1024}"
THROUGHPUT="${THROUGHPUT:--1}"   # -1 = без ограничения

echo ">> Создаю топик $TOPIC"
./scripts/kafka.sh kafka-topics.sh --create --if-not-exists \
  --topic "$TOPIC" --partitions 6 --replication-factor 3 \
  --bootstrap-server "$BOOT" || true

echo
echo ">> PRODUCER PERF TEST"
docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic "$TOPIC" \
  --num-records "$RECORDS" \
  --record-size "$SIZE" \
  --throughput "$THROUGHPUT" \
  --producer-props bootstrap.servers="$BOOT" acks=all linger.ms=10 batch.size=65536 compression.type=lz4

echo
echo ">> CONSUMER PERF TEST"
docker exec -i "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --topic "$TOPIC" \
  --messages "$RECORDS" \
  --bootstrap-server "$BOOT" \
  --group perf-test-group

echo
echo ">> Прогоните с разными acks / linger.ms / compression и запишите"
echo ">> результаты в 09-operations-monitoring/NOTES.md"
