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
