# Этап 11 — Продвинутые паттерны

**Цель:** проектировать системы вокруг Kafka, а не просто пользоваться ей.

## Теория
- «DDIA», гл. 11 — event sourcing, CQRS, потоки как источник истины
- [microservices.io: Transactional Outbox](https://microservices.io/patterns/data/transactional-outbox.html)
- [Docs: Geo-Replication (MirrorMaker 2)](https://kafka.apache.org/documentation/#georeplication)
- **KIP-932** — share groups / очереди (early access в 4.0)
- KIP-405 — tiered storage

## Задачи
- [ ] 1. Реализовать `OutboxPattern`: запись в бизнес-таблицу + outbox в одной
      транзакции БД, публикация через Debezium Outbox Event Router SMT.
- [ ] 2. Доказать надёжность: убить сервис между коммитом БД и публикацией,
      убедиться что событие всё равно доедет.
- [ ] 3. Реализовать `IdempotentConsumer` с дедупликацией, смоделировать дубли.
- [ ] 4. Поднять второй кластер и настроить **MirrorMaker 2**,
      продемонстрировать репликацию топиков и трансляцию оффсетов.
- [ ] 5. (Опц.) Поэкспериментировать с share groups (KIP-932): queue-семантика,
      индивидуальные ack, сравнить с обычной consumer group.

## Контрольные вопросы
1. Почему «сначала БД, потом Kafka» рассинхронизирует системы,
   и как именно outbox это чинит?
2. Outbox даёт exactly-once? Если нет — что требуется от потребителя?
3. Почему in-memory дедупликации недостаточно в продакшене?
4. Как MirrorMaker 2 транслирует оффсеты между кластерами и почему
   их нельзя копировать напрямую?
5. Чем share group принципиально отличается от consumer group?
