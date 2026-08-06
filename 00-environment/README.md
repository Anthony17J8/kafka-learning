# Этап 0 — Окружение

**Цель:** поднять локальный Kafka 4.x в KRaft-режиме и понять, из чего он состоит.

## Теория

- [Официальная документация: KRaft](https://kafka.apache.org/documentation/#kraft) — как Kafka обходится без ZooKeeper
- KIP-500 — исходное предложение по замене ZooKeeper
- «Designing Data-Intensive Applications», гл. 5–6 (репликация, партиционирование) — фундамент
- «Kafka: The Definitive Guide», гл. 1 (общее введение)

## Задачи

- [ ] 1. Установить Docker, Java 17, Maven по инструкции из корневого README.
- [ ] 2. Поднять одиночный брокер: `./scripts/up.sh single`.
- [ ] 3. Поднять кластер из трёх нод: `./scripts/up.sh cluster`.
- [ ] 4. Проверить состояние KRaft-кворума:
      `./scripts/kafka.sh kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --status`
- [ ] 5. Разобраться, что делает `kafka-storage.sh format` и зачем нужен `cluster.id`.
      Подсказка: попробуйте поднять брокер на томе с другим cluster.id.
- [ ] 6. Изучить `docker/single-broker.yml`: объяснить каждый из трёх листенеров.

## Контрольные вопросы

1. Чем combined-режим (`process.roles=broker,controller`) отличается от isolated,
   и когда какой применять в продакшене?
2. Зачем в compose три разных листенера (PLAINTEXT, PLAINTEXT_HOST, CONTROLLER)?
   Что сломается, если оставить один?
3. Что такое `advertised.listeners` и почему клиент с хоста не может
   подключиться по адресу `kafka-1:19093`?
4. Сколько нод контроллера нужно, чтобы пережить отказ одной? двух?

## Результат этапа

Скриншот вывода `kafka-metadata-quorum.sh describe --status` в `screenshots/`
и заполненный `NOTES.md`.
