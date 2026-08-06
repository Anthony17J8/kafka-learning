package dev.kafkalearn.common;

import java.time.Instant;

/**
 * Учебное доменное событие. Сквозной пример для всех этапов:
 * producers -> consumers -> transactions -> streams -> capstone.
 *
 * Ключ сообщения в Kafka = orderId, чтобы все события одного заказа
 * попадали в одну партицию и сохраняли порядок.
 */
public record OrderEvent(
        String orderId,
        String userId,
        String product,
        int quantity,
        double price,
        Status status,
        Instant createdAt
) {

    public enum Status {
        CREATED, PAID, SHIPPED, CANCELLED
    }

    public double total() {
        return quantity * price;
    }

    public OrderEvent withStatus(Status newStatus) {
        return new OrderEvent(orderId, userId, product, quantity, price, newStatus, createdAt);
    }
}
