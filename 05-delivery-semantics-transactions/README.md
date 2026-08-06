# Этап 5 — Семантика доставки и транзакции

**Цель:** реализовать exactly-once обработку и понять её границы.

## Теория
- [Docs: Message Delivery Semantics](https://kafka.apache.org/documentation/#semantics)
- KIP-98 — транзакции, KIP-129 — EOS в Streams, **KIP-890** — усиленная защита транзакций (4.0)
- Статья Confluent «Transactions in Apache Kafka» (Apurva Mehta, Jason Gustafson)

## Задачи
- [ ] 1. Реализовать `TransactionalPipeline`: атомарная запись в два топика.
- [ ] 2. Добавить `sendOffsetsToTransaction` — коммит оффсетов внутри транзакции.
- [ ] 3. Убить процесс между `send` и `commitTransaction`, доказать отсутствие
      частичных записей для `read_committed` консьюмера.
- [ ] 4. Сравнить выдачу `read_committed` vs `read_uncommitted` на одних данных.
- [ ] 5. Запустить два инстанса с **одинаковым** `transactional.id`,
      наблюдать zombie fencing (`ProducerFencedException`).
- [ ] 6. Замерить накладные расходы транзакций на throughput.

## Контрольные вопросы
1. Почему `transactional.id` должен быть стабильным между рестартами?
   Что сломается при генерации UUID на старте?
2. Почему EOS «внутри Kafka» не даёт exactly-once при записи во внешнюю БД?
3. Как `read_committed` консьюмер узнаёт, какие записи пропускать?
4. Что такое LSO (last stable offset) и как он влияет на lag?
5. В чём разница между `exactly_once_v2` в Streams и ручными транзакциями?
