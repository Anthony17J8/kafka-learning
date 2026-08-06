#!/usr/bin/env bash
# Задача 6: compaction сохраняет последнее значение каждого ключа.
set -euo pipefail
cd "$(dirname "$0")/../.."

./scripts/kafka.sh kafka-topics.sh --create --if-not-exists \
  --topic compact-demo --partitions 1 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=5000 \
  --config delete.retention.ms=1000 \
  --config min.compaction.lag.ms=0

echo ">> Пишем несколько версий ключей a, b, c"
printf 'a:1\nb:1\na:2\nc:1\nb:2\na:3\n' | \
  ./scripts/kafka.sh kafka-console-producer.sh --topic compact-demo \
    --property parse.key=true --property key.separator=:

echo ">> Закрываем активный сегмент дозаписью после паузы"
sleep 6
printf 'd:1\n' | \
  ./scripts/kafka.sh kafka-console-producer.sh --topic compact-demo \
    --property parse.key=true --property key.separator=:

echo ">> Ждём проход компактора (15 сек)..."
sleep 15

echo ">> Результат: для a и b остались только последние значения (a=3, b=2)"
./scripts/kafka.sh kafka-console-consumer.sh --topic compact-demo \
  --from-beginning \
  --property print.key=true --property print.offset=true --timeout-ms 5000 || true

echo
echo ">> Если не сжалось с первого раза - допишите ещё пар ключей и подождите."
echo ">> Компакция асинхронна и требует, чтобы 'грязная' часть превысила порог."
