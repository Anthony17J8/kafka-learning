# Этап 2 — Producers

**Цель:** надёжно и эффективно писать в Kafka, понимая цену каждой настройки.

## Теория

- [Docs: Producer configs](https://kafka.apache.org/documentation/#producerconfigs) — читать целиком, это база
- «Kafka: The Definitive Guide», гл. 3
- KIP-98 (идемпотентность и транзакции), KIP-480 (sticky partitioner)

Ключевое: `acks`, `enable.idempotence`, `linger.ms`, `batch.size`, `compression.type`,
`max.in.flight.requests.per.connection`, `delivery.timeout.ms`, партиционирование.

## Код в этой папке

| Класс | Что делает |
|---|---|
| `SimpleProducer` | ✅ рабочий: синхронная и асинхронная отправка |
| `ThroughputBenchmark` | ✅ рабочий: сравнение конфигураций по throughput/latency |
| `CustomPartitioner` | ⚠ заготовка с TODO |

## Задачи

- [ ] 1. Запустить `SimpleProducer`, объяснить вывод (partition/offset для каждой записи).
- [ ] 2. Запустить `ThroughputBenchmark`, записать таблицу результатов в `NOTES.md`.
      Прогнать против single и против кластера — сравнить.
- [ ] 3. Дописать `CustomPartitioner` (все TODO), подключить его и доказать,
      что VIP-ключи попадают в партицию 0.
- [ ] 4. Добавить в бенчмарк p99-латентность и вариант с `compression.type=zstd`.
- [ ] 5. **Эксперимент с отказом:** во время работы продюсера остановить брокер-лидер.
      Наблюдать ретраи. Проверить, потерялись ли сообщения при `acks=all` и при `acks=1`.
- [ ] 6. Выставить `enable.idempotence=false` и `max.in.flight=5`, смоделировать ретрай,
      попробовать поймать нарушение порядка.

## Контрольные вопросы

1. Почему `max.in.flight.requests.per.connection > 1` без идемпотентности
   может переставить сообщения местами при ретрае?
2. `acks=all` гарантирует, что данные не потеряются?
   При каких настройках кластера — нет?
3. Что делает `linger.ms=0` с пропускной способностью и почему?
4. Чем `delivery.timeout.ms` отличается от `request.timeout.ms` и `retries`?
5. Что произойдёт при переполнении `buffer.memory`?

## Результат этапа

Таблица бенчмарков в `NOTES.md`, рабочий `CustomPartitioner`, выводы по отказоустойчивости.
