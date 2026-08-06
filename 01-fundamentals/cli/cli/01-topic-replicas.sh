#!/usr/bin/env bash
# Задача 1: создание топика, изучение распределения реплик и ISR.
set -euo pipefail
cd "$(dirname "$0")/../.."

./scripts/kafka.sh kafka-topics.sh --create --if-not-exists \
  --topic fundamentals --partitions 3 --replication-factor 3

echo ">> Распределение реплик, лидеры, ISR:"
./scripts/kafka.sh kafka-topics.sh --describe --topic fundamentals

echo
echo ">> Наблюдения для NOTES.md:"
echo "   - лидеры разложены по разным брокерам (балансировка)"
echo "   - первая реплика в Replicas = preferred leader"
echo "   - Isr = 1,2,3 => полный запас синхронных реплик"
