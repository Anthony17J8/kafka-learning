#!/usr/bin/env bash
# Задача 5: retention удаляет закрытые сегменты старше порога, активный не трогает.
set -euo pipefail
cd "$(dirname "$0")/../.."

./scripts/kafka.sh kafka-topics.sh --create --if-not-exists \
  --topic retention-demo --partitions 1 --replication-factor 3 \
  --config retention.ms=30000 \
  --config segment.ms=10000 \
  --config segment.bytes=1024

echo ">> Пишем 50 сообщений"
for i in $(seq 1 50); do echo "message-$i"; done | \
  ./scripts/kafka.sh kafka-console-producer.sh --topic retention-demo

LEADER=$(./scripts/kafka.sh kafka-topics.sh --describe --topic retention-demo \
  | awk '/Partition: 0/ {for(i=1;i<=NF;i++) if($i=="Leader:") print "kafka-"$(i+1)}')
echo ">> Лидер партиции: $LEADER"

echo ">> Сегменты СЕЙЧАС:"
docker exec "$LEADER" ls -la /var/lib/kafka/data/retention-demo-0/ | grep -E '\.log|\.index' || true

echo ">> Ждём 70 секунд (retention + проверка)..."
sleep 70

echo ">> Сегменты ПОСЛЕ retention (старые удалены):"
docker exec "$LEADER" ls -la /var/lib/kafka/data/retention-demo-0/ | grep -E '\.log|\.index' || true

echo ">> Начало лога сдвинулось вперёд:"
./scripts/kafka.sh kafka-get-offsets.sh --topic retention-demo --time earliest
