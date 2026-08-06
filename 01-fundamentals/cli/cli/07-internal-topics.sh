#!/usr/bin/env bash
# Задача 7: внутренние топики и разбор структуры сегмента.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo ">> Все топики (включая внутренние):"
./scripts/kafka.sh kafka-topics.sh --list

echo
echo ">> __consumer_offsets: 50 партиций, cleanup.policy=compact"
./scripts/kafka.sh kafka-topics.sh --describe --topic __consumer_offsets | head -3

echo
echo ">> Разбор структуры батчей на диске (kafka-dump-log):"
docker exec kafka-1 sh -c '/opt/kafka/bin/kafka-dump-log.sh \
  --files /var/lib/kafka/data/fundamentals-0/00000000000000000000.log \
  --print-data-log' 2>/dev/null | head -15 || \
  echo "   (партиция fundamentals-0 может быть не на kafka-1 - проверьте лидера)"
