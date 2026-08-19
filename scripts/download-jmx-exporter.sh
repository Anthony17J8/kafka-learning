#!/usr/bin/env bash
# Скачивает JMX Prometheus javaagent (нужен для этапа 9).
set -euo pipefail
cd "$(dirname "$0")/.."

VERSION="${JMX_EXPORTER_VERSION:-1.0.1}"
TARGET="docker/jmx-exporter/jmx_prometheus_javaagent.jar"
URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${VERSION}/jmx_prometheus_javaagent-${VERSION}.jar"

if [ -f "$TARGET" ]; then
  echo ">> Уже скачан: $TARGET"
  exit 0
fi

echo ">> Скачиваю JMX exporter ${VERSION}..."
curl -fSL "$URL" -o "$TARGET"
echo ">> Готово: $TARGET"
