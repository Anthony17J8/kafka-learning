package dev.kafkalearn.capstone;

/**
 * CAPSTONE — ЗАГОТОВКА финального проекта.
 *
 * Real-time система заказов и аналитики. Должна использовать всё изученное:
 *   1. Ingestion: сервис заказов -> Postgres + outbox -> Debezium -> Kafka
 *      (Avro + Schema Registry).
 *   2. Обработка: Kafka Streams — join с пользователями, оконные агрегаты,
 *      processing.guarantee = exactly_once_v2.
 *   3. Sink: Kafka Connect -> витрина + архив.
 *   4. Надёжность: RF=3, min.insync.replicas=2, acks=all.
 *   5. Наблюдаемость: JMX -> Prometheus -> Grafana, алерт по consumer lag.
 *   6. Безопасность: TLS + SASL/SCRAM + ACL.
 *
 * Критерии готовности — в корневом README.md.
 */
public class CapstoneApplication {

    public static void main(String[] args) {
        throw new UnsupportedOperationException("Capstone: соберите систему сами");
    }
}
