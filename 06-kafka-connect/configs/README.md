# Конфигурации коннекторов

## Управление через REST API

```bash
CONNECT=http://localhost:8083

# Список установленных плагинов
curl -s $CONNECT/connector-plugins | jq '.[].class'

# Создать коннектор
curl -s -X POST -H "Content-Type: application/json" \
     --data @debezium-postgres-source.json $CONNECT/connectors | jq

# Статус
curl -s $CONNECT/connectors/postgres-source/status | jq

# Пауза / возобновление
curl -s -X PUT $CONNECT/connectors/postgres-source/pause
curl -s -X PUT $CONNECT/connectors/postgres-source/resume

# Рестарт упавшего таска
curl -s -X POST $CONNECT/connectors/postgres-source/tasks/0/restart

# Удалить
curl -s -X DELETE $CONNECT/connectors/postgres-source
```

## Проверка CDC

```bash
# В отдельном терминале - слушаем топик
KAFKA_CONTAINER=kafka-1 BOOTSTRAP=kafka-1:19093 \
  ../../scripts/kafka.sh kafka-console-consumer.sh --topic cdc.shop.orders --from-beginning

# Меняем данные в БД
docker exec -it postgres psql -U kafka -d shop -c \
  "INSERT INTO shop.orders (customer_id, product, quantity, price) VALUES (1,'laptop',1,1499.00);"
```

## TODO этапа 6

- [ ] Sink-коннектор: Kafka -> Elasticsearch / JDBC / S3(MinIO)
- [ ] SMT: маскирование email в `cdc.shop.customers`
- [ ] DLQ: отправить битое сообщение и найти его в топике `dlq`
