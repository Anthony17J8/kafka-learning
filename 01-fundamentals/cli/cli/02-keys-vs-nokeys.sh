#!/usr/bin/env bash
# Задача 2: ключ определяет партицию. Одинаковый ключ -> одна партиция.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo ">> Пишем с ключами (parse.key=true, разделитель ':')"
printf 'user-1:order-A\nuser-1:order-B\nuser-2:order-C\nuser-1:order-D\nuser-2:order-E\n' | \
  ./scripts/kafka.sh kafka-console-producer.sh --topic fundamentals \
    --property parse.key=true --property key.separator=:

echo ">> Читаем обратно с указанием партиции каждой записи:"
./scripts/kafka.sh kafka-console-consumer.sh --topic fundamentals \
  --from-beginning \
  --property print.key=true --property print.partition=true --timeout-ms 6000 || true

echo
echo ">> Ожидание: все user-1 в одной партиции, все user-2 в другой."
echo ">> Порядок внутри user-1 (A,B,D) сохранён."

echo
echo ">> Теперь без ключей (sticky-партиционер):"
printf 'no-key-1\nno-key-2\nno-key-3\nno-key-4\n' | \
  ./scripts/kafka.sh kafka-console-producer.sh --topic fundamentals
