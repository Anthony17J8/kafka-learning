package dev.kafkalearn.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

/**
 * ЭТАП 7 — ЗАГОТОВКА. Топология Kafka Streams.
 *
 * Топология вынесена в отдельный метод специально: так её можно тестировать
 * через TopologyTestDriver без запуска реального кластера (задача 7).
 *
 * Задачи этапа:
 *   [ ] 1. Word/event count как разминка.
 *   [ ] 2. Агрегация в tumbling-окне 1 минута: заказов и выручки по userId.
 *   [ ] 3. Join: KStream(orders) x KTable(users) -> orders-enriched.
 *   [ ] 4. Stateful-детектор: алерт при > N заказов за окно.
 *   [ ] 5. processing.guarantee = exactly_once_v2, проверить рестартом.
 *   [ ] 6. Interactive Queries: отдать состояние стора по HTTP.
 *   [ ] 7. Тесты через TopologyTestDriver.
 *
 * Ключевой вопрос: почему для stream-table join нужна ко-партиционированность?
 */
public final class OrdersTopology {

    public static final String ORDERS_TOPIC = "orders";
    public static final String USERS_TOPIC = "users";
    public static final String ENRICHED_TOPIC = "orders-enriched";
    public static final String METRICS_TOPIC = "metrics";
    public static final String COUNT_STORE = "orders-per-user-store";

    private OrdersTopology() {
    }

    public static Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        // TODO 1: builder.stream(ORDERS_TOPIC, Consumed.with(...))
        // TODO 2: .groupByKey().windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
        // TODO 3: .join(usersTable, ...)
        // TODO 4: .toStream().to(METRICS_TOPIC, Produced.with(...))

        return builder.build();
    }
}
