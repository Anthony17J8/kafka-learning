# Этап 4 — Сериализация и Schema Registry

**Цель:** перестать гонять «сырой JSON» и научиться управлять эволюцией схем.

## Теория
- [Confluent: Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
- [Avro specification](https://avro.apache.org/docs/current/specification/)
- «Kafka: The Definitive Guide», гл. 3 (раздел про сериализацию)
- «DDIA», гл. 4 — лучшее объяснение эволюции схем, что существует

## Окружение
```bash
./scripts/up.sh core        # 3 брокера + Schema Registry
curl -s http://localhost:8081/subjects | jq
```

## Задачи
- [ ] 1. Зарегистрировать `order-event.avsc` как subject `orders-value`.
- [ ] 2. Написать продюсер/консьюмер с `KafkaAvroSerializer`/`KafkaAvroDeserializer`.
- [ ] 3. Зарегистрировать `order-event-v2.avsc` (поле с default) → должно пройти.
- [ ] 4. Зарегистрировать `order-event-v3-broken.avsc` → должно быть **отклонено**.
      Сохранить текст ошибки в `NOTES.md`.
- [ ] 5. Проверить совместимость до регистрации:
      `POST /compatibility/subjects/orders-value/versions/latest`
- [ ] 6. Повторить с Protobuf или JSON Schema, сравнить опыт.
- [ ] 7. Составить таблицу: «изменение схемы × режим совместимости → можно/нельзя».

## Контрольные вопросы
1. Почему BACKWARD — самый частый выбор по умолчанию?
2. Что физически лежит в первых 5 байтах Avro-сообщения и зачем?
3. Почему добавление поля без `default` ломает BACKWARD, но не FORWARD?
4. Что произойдёт, если Schema Registry недоступен: продюсер упадёт? консьюмер?
