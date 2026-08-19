# Конспект — 04-serialization-schema-registry

### Задача 2

При чтении консольным консьюмером ``kafka-console-consumer`` следующая картина:
```js
Hb1dee2cb-2422-449b-a1c9-0876d2b87738H1eeb7414-43f5-4a6d-bea3-ef0e958f8c1eShampoo�?�򦟃h
```
При чтении консьюмером из контейнера с предустановленными утилитами confluent образа,
```bash
docker exec -it schema-registry kafka-avro-console-consumer --bootstrap-server kafka-1:19093 --topic orders-avro --from-beginning --max-messages 1 --property schema.registry.url=http://localhost:8081 2>/dev/null | grep '^{'
```
Получаем следующий результат:
```js
{"orderId":"1fe52181-6fe9-4db1-9607-012e6cd61819","userId":"843863ad-8c8f-4abe-9d49-20b93cc4720d","product":"Shampoo","quantity":10,"price":1.0,"status":"CREATED","createdAt":1787141872421}
```

### Задача 3

При выполнении десериализатором ***schema resolution*** (в режиме BACKWARD) выполнили следующие шаги:

- выполнили импорт схемы для ``OrderEvent`` вручную в реестр схем (добавили новое поле)
- скомпилировали консьюмер на новой схеме
- в результате при чтении консьюмером данных записанных при старой схеме, получаем корректный результат, в качестве
  значения для нового поля указано defaul-значение

### Задача 4

При использовании несовместимой версии схемы:
```json
{
  "error_code": 409,
  "message": "Schema being registered is incompatible with an earlier schema for subject \"orders-avro-value\", details: [{errorType:'TYPE_MISMATCH', description:'The type (path '/fields/4/type') of a field in the new schema does not match with the old schema', additionalInfo:'reader type: STRING not compatible with writer type: DOUBLE'}, {errorType:'READER_FIELD_MISSING_DEFAULT_VALUE', description:'The field 'warehouseId' at path '/fields/7' in the new schema has no default value and is missing in the old schema', additionalInfo:'warehouseId'}, {oldSchemaVersion: 4}, {oldSchema: '{\"type\":\"record\",\"name\":\"OrderEvent\",\"namespace\":\"dev.kafkalearn.avro\",\"doc\":\"ВЕРСИЯ 2. Добавлено поле currency С DEFAULT -> BACKWARD-совместимо.\",\"fields\":[{\"name\":\"orderId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"userId\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"product\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"}},{\"name\":\"quantity\",\"type\":\"int\"},{\"name\":\"price\",\"type\":\"double\"},{\"name\":\"status\",\"type\":{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"CREATED\",\"PAID\",\"SHIPPED\",\"CANCELLED\"]}},{\"name\":\"createdAt\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"currency\",\"type\":{\"type\":\"string\",\"avro.java.string\":\"String\"},\"default\":\"EUR\"}]}'}, {validateFields: 'false', compatibility: 'BACKWARD'}]"
}
```
**Проблема 1:** изменение несовместимого типа данных (double -> String)  
**Проблема 2:** добавление нового поля без указания default-значения  
**Проверка совместимости — это симуляция чтения старых данных новой схемой**

### Задача 6
```json
{
  "brokers": [
    {
      "broker": 1,
      "logDirs": [
        {
          "partitions": [
            {
              "partition": "orders-avro-0",
              "size": 1045,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-json-2",
              "size": 909,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-avro-1",
              "size": 1052,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-avro-2",
              "size": 553,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-proto-2",
              "size": 561,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-proto-1",
              "size": 1127,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-proto-0",
              "size": 1061,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-json-0",
              "size": 1757,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-json-1",
              "size": 1818,
              "offsetLag": 0,
              "isFuture": false
            }
          ],
          "error": null,
          "logDir": "/var/lib/kafka/data"
        }
      ]
    },
    {
      "broker": 2,
      "logDirs": [
        {
          "partitions": [
            {
              "partition": "orders-avro-0",
              "size": 1045,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-json-2",
              "size": 909,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-avro-1",
              "size": 1052,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-avro-2",
              "size": 553,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-proto-2",
              "size": 561,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-proto-1",
              "size": 1127,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-proto-0",
              "size": 1061,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-json-0",
              "size": 1757,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-json-1",
              "size": 1818,
              "offsetLag": 0,
              "isFuture": false
            }
          ],
          "error": null,
          "logDir": "/var/lib/kafka/data"
        }
      ]
    },
    {
      "broker": 3,
      "logDirs": [
        {
          "partitions": [
            {
              "partition": "orders-avro-0",
              "size": 1045,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-json-2",
              "size": 909,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-avro-1",
              "size": 1052,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-avro-2",
              "size": 553,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-proto-2",
              "size": 561,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-proto-1",
              "size": 1127,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-proto-0",
              "size": 1061,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-json-0",
              "size": 1757,
              "offsetLag": 0,
              "isFuture": false
            },
            {
              "partition": "orders-json-1",
              "size": 1818,
              "offsetLag": 0,
              "isFuture": false
            }
          ],
          "error": null,
          "logDir": "/var/lib/kafka/data"
        }
      ]
    }
  ],
  "version": 1
}

```

