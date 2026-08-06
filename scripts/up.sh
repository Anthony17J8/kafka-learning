#!/usr/bin/env bash
# Поднять окружение нужного уровня.
#   ./scripts/up.sh single      - 1 брокер + UI
#   ./scripts/up.sh cluster     - 3 брокера + UI
#   ./scripts/up.sh core        - 3 брокера + Schema Registry + UI
#   ./scripts/up.sh connect     - + Postgres + Kafka Connect (Debezium)
#   ./scripts/up.sh monitoring  - + Prometheus + Grafana
#   ./scripts/up.sh all         - всё сразу (тяжело!)
set -euo pipefail
cd "$(dirname "$0")/.."

MODE="${1:-single}"

case "$MODE" in
  single)
    docker compose -f docker/single-broker.yml up -d
    ;;
  cluster)
    docker compose -f docker/three-broker.yml up -d
    ;;
  core|connect|monitoring|ksql|all)
    if [ ! -f docker/jmx-exporter/jmx_prometheus_javaagent.jar ]; then
      echo ">> JMX exporter не найден, скачиваю..."
      ./scripts/download-jmx-exporter.sh
    fi
    docker compose -f docker/full-stack.yml --profile "$MODE" up -d
    ;;
  *)
    echo "Неизвестный режим: $MODE" >&2
    echo "Доступно: single | cluster | core | connect | monitoring | ksql | all" >&2
    exit 1
    ;;
esac

echo
echo "=== Готово. Полезные адреса ==="
echo "  Kafka UI     : http://localhost:8080"
echo "  Schema Reg.  : http://localhost:8081  (профили core/connect/all)"
echo "  Connect REST : http://localhost:8083  (профили connect/all)"
echo "  Prometheus   : http://localhost:9090  (профили monitoring/all)"
echo "  Grafana      : http://localhost:3000  (admin/admin)"
