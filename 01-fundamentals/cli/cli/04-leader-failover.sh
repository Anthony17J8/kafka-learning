#!/usr/bin/env bash
# Задача 4: остановка лидера, наблюдение переизбрания и сжатия ISR.
# ВНИМАНИЕ: скрипт останавливает kafka-1. Точка входа переключается на kafka-2.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo ">> СОСТОЯНИЕ ДО (через kafka-1):"
KAFKA_CONTAINER=kafka-1 BOOTSTRAP=kafka-1:19093 \
  ./scripts/kafka.sh kafka-topics.sh --describe --topic fundamentals

echo
echo ">> Останавливаем kafka-1..."
docker stop kafka-1
sleep 5

echo
echo ">> СОСТОЯНИЕ ПОСЛЕ (через kafka-2): лидеры переехали, ISR сократился"
KAFKA_CONTAINER=kafka-2 BOOTSTRAP=kafka-2:19093 \
  ./scripts/kafka.sh kafka-topics.sh --describe --topic fundamentals

echo
echo ">> Возвращаем kafka-1..."
docker start kafka-1
echo ">> Ждём восстановления ISR (30 сек)..."
sleep 30

echo
echo ">> СОСТОЯНИЕ ПОСЛЕ ВОЗВРАТА: ISR снова полный"
KAFKA_CONTAINER=kafka-2 BOOTSTRAP=kafka-2:19093 \
  ./scripts/kafka.sh kafka-topics.sh --describe --topic fundamentals

echo
echo ">> Форсируем возврат лидерства preferred-репликам:"
KAFKA_CONTAINER=kafka-2 BOOTSTRAP=kafka-2:19093 \
  ./scripts/kafka.sh kafka-leader-election.sh \
    --election-type PREFERRED --all-topic-partitions || true

echo ">> Не забудьте вернуть в терминале: export KAFKA_CONTAINER=kafka-1; export BOOTSTRAP=kafka-1:19093"
