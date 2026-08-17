# Конспект — 03-consumers

## Результаты исследований

### Задача 1

Консьюмер получил все три партиции: координатор группы при JoinGroup
увидел единственного участника и назначил ему всё — делить не с кем.
Назначение пришло в ```onPartitionsAssigned```, отсюда строка ```ASSIGNED``` в логе.

Наблюдения из вывода:

- оффсеты в каждой партиции нумеруются независимо (p=0 off=15, p=1 off=203);
- записи одного ключа приходят из одной партиции, порядок внутри сохранён;
- записи разных партиций перемежаются — глобального порядка нет.

### Задача 2,3

При запуске чтения consumer 1 ему назначаются все партиции ```([orders-0, orders-1, orders-2])```.
При присодинении consumer 2 проихсодит ребалансировка и все consumers отдают свои партиции (наблюдаем
REVOKED: ```[orders-0, orders-1, orders-2]```). Каждый consumer шлет ```JoinGroup``` координатору, координатор в свою
очередь выбирает лидера среди consumers и consumer-лидер вычисляет назначение партиций и шлет координатору
```SyncGroup```.
Координатор
рассылает назначения (наблюдаем ```consumer-1 ASSIGNED: [orders-2]```,
```consumer-2 ASSIGNED: [orders-0, orders-1] ```).
Дальше появляется consumer 3 - выполняются аналогичные шаги, что в логе:

```
consumer-1 REVOKED: [orders-2]
consumer-2 REVOKED: [orders-0, orders-1]
consumer-1 ASSIGNED: [orders-1]
consumer-2 ASSIGNED: [orders-0]
consumer-3 ASSIGNED: [orders-2]
```

Далее появляется consumer 4 - в логах следующее:

```
consumer-1 REVOKED: [orders-1]
consumer-2 REVOKED: [orders-0]
consumer-3 REVOKED: [orders-2]
consumer-1 ASSIGNED: [orders-2]
consumer-2 ASSIGNED: [orders-1]
consumer-3 ASSIGNED: []
consumer-4 ASSIGNED: [order-0]
```

По итогу один consumer не получил партиции, т.к. ```Nпартиций < Nconsumer```.
Консьюмеры сверх числа параллелизма (=число партиций) простаивают не получая назначения.

### Задача 4

Главный результат — не цифры, а количество отданных партиций:

| Протокол           | Отдал  | при добавлении 2-го консьюмера        |
|--------------------|--------|---------------------------------------|
| classic (eager)    | 3 из 3 | — вся работа встала                   |
| consumer (KIP-848) | 2 из 3 | — одна партиция читалась без перерыва |

Замер revoke → assign включает ожидание poll() и потому несопоставим между протоколами.

### Задача 7
```js
20:06:51.500 WARN  [consumer_background_thread] o.a.k.c.c.i.ConsumerHeartbeatRequestManager - [Consumer clientId=consumer-task71-1, groupId=task71] Time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, exceeded approximately by 15032 ms. Member 4uzN8uR5SN2P4aq_Ah_n4Q will rejoin the group now.
20:06:51.500 ERROR [dev.kafkalearn.consumers.ManualCommitConsumer.main()] d.k.c.ManualCommitConsumer - LOST: [orders-0, orders-1, orders-2]
20:06:51.506 ERROR [consumer_background_thread] o.a.k.c.c.i.CommitRequestManager - [Consumer clientId=consumer-task71-1, groupId=task71] Unexpected error handling response for OffsetCommit request for offsets {}
java.lang.NullPointerException: Cannot invoke "org.apache.kafka.clients.consumer.OffsetAndMetadata.offset()" because "offsetAndMetadata" is null
	at org.apache.kafka.clients.consumer.internals.CommitRequestManager$OffsetCommitRequestState.onResponse(CommitRequestManager.java:755)
	at org.apache.kafka.clients.consumer.internals.CommitRequestManager$RetriableRequestState.handleClientResponse(CommitRequestManager.java:909)
	at org.apache.kafka.clients.consumer.internals.CommitRequestManager$RetriableRequestState.lambda$buildRequestWithResponseHandling$0(CommitRequestManager.java:899)
	at java.base/java.util.concurrent.CompletableFuture.uniWhenComplete(CompletableFuture.java:863)
	at java.base/java.util.concurrent.CompletableFuture$UniWhenComplete.tryFire(CompletableFuture.java:841)
	at java.base/java.util.concurrent.CompletableFuture.postComplete(CompletableFuture.java:510)
	at java.base/java.util.concurrent.CompletableFuture.complete(CompletableFuture.java:2147)
	at org.apache.kafka.clients.consumer.internals.NetworkClientDelegate$FutureCompletionHandler.onComplete(NetworkClientDelegate.java:433)
	at org.apache.kafka.clients.ClientResponse.onComplete(ClientResponse.java:154)
	at org.apache.kafka.clients.NetworkClient.completeResponses(NetworkClient.java:669)
	at org.apache.kafka.clients.NetworkClient.poll(NetworkClient.java:661)
	at org.apache.kafka.clients.consumer.internals.NetworkClientDelegate.poll(NetworkClientDelegate.java:153)
	at org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread.runOnce(ConsumerNetworkThread.java:162)
	at org.apache.kafka.clients.consumer.internals.ConsumerNetworkThread.run(ConsumerNetworkThread.java:106)
20:06:52.002 WARN  [dev.kafkalearn.consumers.ManualCommitConsumer.main()] d.k.c.ManualCommitConsumer - Асинхронный коммит не удался: Cannot invoke "org.apache.kafka.clients.consumer.OffsetAndMetadata.offset()" because "offsetAndMetadata" is null
```
По логам видно, что timeout между вызовами poll был превышен (``max.poll.interval.ms``). Консьюмер теряет свои партиции, 
которые были ему назначены. При последующем вызове poll консьюмер шлет joinGroup координатору и ему снова назначаются партиции. 
И так по кругу. Почему сработал LOST а не REVOKED? В случае с LOST commit не имеет смысл, т.к. коммит может затереть 
offset после обработки новым владельцем партиции. 
