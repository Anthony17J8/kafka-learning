package dev.kafkalearn.streams;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * ЭТАП 7, задача 7 — ЗАГОТОВКА теста топологии.
 *
 * TopologyTestDriver гоняет топологию в памяти: ни брокера, ни Docker,
 * ни задержек. Идеально для CI.
 *
 * Каркас:
 *   var driver = new TopologyTestDriver(OrdersTopology.build(), props);
 *   var input  = driver.createInputTopic("orders", keySerde.serializer(), valSerde.serializer());
 *   var output = driver.createOutputTopic("orders-enriched", keySerde.deserializer(), valSerde.deserializer());
 *   input.pipeInput("user-1", order);
 *   assertThat(output.readValue()).isEqualTo(expected);
 *
 * Не забудьте driver.close() (лучше через @AfterEach).
 * Для оконных агрегаций используйте input.pipeInput(key, value, timestamp),
 * чтобы управлять event-time вручную.
 */
class OrdersTopologyTest {

    @Test
    @Disabled("ЭТАП 7: снимите @Disabled после реализации топологии")
    void shouldEnrichOrdersWithUserData() {
        throw new UnsupportedOperationException("Реализуйте на этапе 7");
    }
}
