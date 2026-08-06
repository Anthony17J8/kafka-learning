#!/usr/bin/env bash
# Задача 3: чтение с конкретной партиции и оффсета; границы лога.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo ">> Конец лога (LEO) каждой партиции:"
./scripts/kafka.sh kafka-get-offsets.sh --topic fundamentals --time latest

echo ">> Начало лога (Log Start Offset):"
./scripts/kafka.sh kafka-get-offsets.sh --topic fundamentals --time earliest

echo
echo ">> Чтение партиции 0 начиная с оффсета 1:"
./scripts/kafka.sh kafka-console-consumer.sh --topic fundamentals \
  --partition 0 --offset 1 \
  --property print.offset=true --property print.key=true --timeout-ms 5000 || true
