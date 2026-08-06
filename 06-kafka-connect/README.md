# Этап 6 — Kafka Connect

**Цель:** строить интеграции декларативно, без написания клиентов.

## Теория
- [Docs: Kafka Connect](https://kafka.apache.org/documentation/#connect)
- [Debezium documentation](https://debezium.io/documentation/)
- «Kafka: The Definitive Guide», гл. 9
- Confluent Developer: «Kafka Connect 101»

## Окружение
```bash
./scripts/up.sh connect     # + Postgres + Connect (Debezium)
curl -s http://localhost:8083/connector-plugins | jq '.[].class'
```

## Задачи
- [ ] 1. Поднять Connect, изучить список доступных плагинов.
- [ ] 2. Создать Debezium source из `configs/debezium-postgres-source.json`,
      проверить статус, увидеть CDC-события при изменении данных в Postgres.
- [ ] 3. Настроить sink-коннектор (JDBC / Elasticsearch / S3-MinIO на выбор).
- [ ] 4. Применить SMT: замаскировать email в `cdc.shop.customers`.
- [ ] 5. Настроить DLQ, отправить заведомо битое сообщение, найти его в топике `dlq`
      вместе с заголовками ошибки.
- [ ] 6. Убить контейнер Connect в середине работы, перезапустить,
      убедиться что обработка продолжилась с нужного места. Где хранится это состояние?
- [ ] 7. Все команды REST API оформить в `configs/README.md`.

## Контрольные вопросы
1. Что хранится в `_connect_configs`, `_connect_offsets`, `_connect_status`?
2. Чем `tasks.max` отличается от числа партиций и как они связаны?
3. Зачем Postgres нужен `wal_level=logical` для Debezium?
4. Что произойдёт с replication slot, если Connect надолго остановить?
   (Подсказка: это классический инцидент — WAL растёт и забивает диск.)
5. Когда стоит написать свой консьюмер вместо sink-коннектора?
