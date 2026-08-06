# Этап 8 — ksqlDB (опционально)

**Цель:** тот же stream processing, но декларативно на SQL.

## Теория
- [ksqlDB documentation](https://docs.ksqldb.io/)
- «Mastering Kafka Streams and ksqlDB» (Seymour), часть про ksqlDB
- Confluent Developer: «ksqlDB 101»

## Окружение
```bash
./scripts/up.sh ksql
docker exec -it ksqldb ksql http://localhost:8088
```

## Задачи
- [ ] 1. Объявить STREAM поверх топика `orders`.
- [ ] 2. Повторить оконную агрегацию из этапа 7 на SQL.
- [ ] 3. Сделать join двух потоков.
- [ ] 4. Push-запрос (`EMIT CHANGES`) vs pull-запрос — попробовать оба.
- [ ] 5. Посмотреть, какие Kafka Streams-приложения ksqlDB создал под капотом.

## Контрольные вопросы
1. Когда ksqlDB предпочтительнее нативного Streams, а когда наоборот?
2. Чем STREAM отличается от TABLE в терминах семантики?
3. Что происходит с запросом при рестарте ksqlDB-сервера?
