#!/usr/bin/env bash
# Остановить все окружения. С флагом -v удаляет тома (данные!).
set -euo pipefail
cd "$(dirname "$0")/.."

EXTRA=""
if [ "${1:-}" = "-v" ]; then
  EXTRA="-v"
  echo ">> Внимание: удаляются тома с данными."
fi

for f in docker/single-broker.yml docker/three-broker.yml docker/full-stack.yml; do
  docker compose -f "$f" --profile all down $EXTRA --remove-orphans 2>/dev/null || true
done
echo ">> Остановлено."
