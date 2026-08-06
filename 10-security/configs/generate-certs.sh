#!/usr/bin/env bash
# ЭТАП 10, задача 1 — ЗАГОТОВКА. Генерация самоподписанного CA и keystore/truststore.
#
# ВНИМАНИЕ: сгенерированные файлы попадают в .gitignore. В git секреты не коммитим!
#
# Порядок:
#   1. Создать CA (openssl req -new -x509 ...)
#   2. Для каждого брокера: keystore, CSR, подписать CA, импортировать цепочку
#   3. Truststore с CA для клиентов
#   4. Прописать в брокере: listeners SSL://, ssl.keystore.location и т.д.
#
# TODO: реализуйте. Официальная инструкция:
#   https://kafka.apache.org/documentation/#security_ssl
set -euo pipefail
echo "ЭТАП 10: реализуйте генерацию сертификатов самостоятельно"
exit 1
