# Этап 9 — Эксплуатация, мониторинг, производительность

**Цель:** держать кластер здоровым и осознанно его тюнить.

## Теория
- [Docs: Operations](https://kafka.apache.org/documentation/#operations)
- [Docs: Monitoring](https://kafka.apache.org/documentation/#monitoring) — список JMX-метрик
- «Kafka: The Definitive Guide», гл. 7–10
- KIP-405 — tiered storage

## Окружение
```bash
./scripts/download-jmx-exporter.sh
./scripts/up.sh monitoring
# Prometheus http://localhost:9090   Grafana http://localhost:3000 (admin/admin)
```

## Задачи
- [ ] 1. Убедиться, что Prometheus видит все три брокера (Status → Targets).
- [ ] 2. Собрать дашборд Grafana: throughput, request latency, under-replicated
      partitions, ActiveControllerCount, consumer lag. Экспортировать JSON в `dashboards/`.
- [ ] 3. Прогнать `loadtest/perf-test.sh` с разными настройками,
      найти узкое место, зафиксировать цифры.
- [ ] 4. Смоделировать отставание консьюмера, настроить алерт по lag.
- [ ] 5. `min.insync.replicas=2` при RF=3: остановить один брокер (запись идёт),
      остановить второй (запись падает с `NotEnoughReplicasException`). Задокументировать.
- [ ] 6. Провести реассайн партиций через `kafka-reassign-partitions.sh`,
      описать процедуру пошагово.
- [ ] 7. Изучить `kafka-log-dirs.sh` — посмотреть реальный размер данных на диске.

## Контрольные вопросы
1. Какие 5 метрик вы бы вывели на «дежурный» дашборд и почему именно эти?
2. Что означает ненулевой `UnderReplicatedPartitions` и с чего начинать разбор?
3. Что такое unclean leader election и почему по умолчанию он выключен?
4. Почему «больше партиций» не всегда значит «быстрее»?
5. Как оценить нужное число партиций для целевого throughput?
