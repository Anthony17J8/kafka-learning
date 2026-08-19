#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Обёртка над CLI-скриптами Kafka внутри контейнера.
# Позволяет не устанавливать дистрибутив Kafka на хост.
#
# Примеры:
#   ./scripts/kafka.sh kafka-topics.sh --list
#   ./scripts/kafka.sh kafka-topics.sh --create --topic demo --partitions 3 --replication-factor 3
#   ./scripts/kafka.sh kafka-consumer-groups.sh --describe --group my-group
#
# Контейнер и bootstrap можно переопределить переменными окружения:
#   KAFKA_CONTAINER=kafka-1 BOOTSTRAP=kafka-1:19093 ./scripts/kafka.sh kafka-topics.sh --list
# ---------------------------------------------------------------------------
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"

if [ $# -eq 0 ]; then
  echo "Usage: $0 <kafka-cli-script> [args...]" >&2
  echo "Например: $0 kafka-topics.sh --list" >&2
  exit 1
fi

CMD="$1"; shift

# Если пользователь не передал --bootstrap-server, подставляем сами
if [[ " $* " != *" --bootstrap-server "* ]]; then
  set -- --bootstrap-server "$BOOTSTRAP" "$@"
fi

exec docker exec -i -e KAFKA_OPTS= "$KAFKA_CONTAINER" "/opt/kafka/bin/${CMD}" "$@"
