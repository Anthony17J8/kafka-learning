# Этап 7 — Kafka Streams

**Цель:** обрабатывать потоки декларативно, с состоянием и корректным временем.

## Теория
- [Docs: Kafka Streams](https://kafka.apache.org/documentation/streams/)
- **«Kafka Streams in Action», 2-е изд.** (Bejeck) — основная книга по теме
- **«Mastering Kafka Streams and ksqlDB»** (Seymour)
- «DDIA», гл. 11 — event time vs processing time

## Задачи
- [ ] 1. Реализовать `OrdersTopology.build()`: базовый count.
- [ ] 2. Оконная агрегация: заказы и выручка по `userId` в tumbling-окне 1 мин.
- [ ] 3. Join `KStream(orders)` × `KTable(users)` → `orders-enriched`.
- [ ] 4. Stateful-детектор: алерт при > N заказов пользователя за окно.
- [ ] 5. Включить `processing.guarantee=exactly_once_v2`, проверить рестартом.
- [ ] 6. Interactive Queries: отдать содержимое state store по HTTP.
- [ ] 7. Написать тесты через `TopologyTestDriver` (снять `@Disabled`).
- [ ] 8. Посмотреть, какие внутренние топики создались (`-changelog`, `-repartition`),
      объяснить назначение каждого.

## Контрольные вопросы
1. Почему для stream-table join нужна ко-партиционированность?
   Что произойдёт при разном числе партиций у топиков?
2. Зачем нужен changelog-топик, если состояние и так в RocksDB?
3. Что такое grace period и как он взаимодействует с опоздавшими событиями?
4. В чём разница между `KTable` и `GlobalKTable` по стоимости и семантике join?
5. Почему `exactly_once_v2` в Streams «дешевле», чем ручные транзакции?
