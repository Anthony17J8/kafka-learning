#!/usr/bin/env bash
# Этап 1: подготовка. Запускать из корня проекта.
# Поднимает кластер и задаёт переменные окружения для остальных скриптов.
set -euo pipefail
cd "$(dirname "$0")/../.."

./scripts/up.sh cluster
echo ">> Ждём готовности брокеров..."
sleep 5
docker ps --format 'table {{.Names}}\t{{.Status}}' | grep kafka

cat <<'HINT'

Теперь в вашем терминале выполните:
  export KAFKA_CONTAINER=kafka-1
  export BOOTSTRAP=kafka-1:19093

И запускайте скрипты по порядку: 01, 02, ...
HINT
