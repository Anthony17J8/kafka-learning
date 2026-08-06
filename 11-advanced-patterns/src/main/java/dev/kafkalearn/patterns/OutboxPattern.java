package dev.kafkalearn.patterns;

/**
 * ЭТАП 11 — ЗАГОТОВКА. Transactional Outbox.
 *
 * Проблема (dual write): нельзя атомарно записать в БД и в Kafka.
 *   - Записали в БД, упали до Kafka -> событие потеряно.
 *   - Записали в Kafka, упала транзакция БД -> событие о несуществующем факте.
 *
 * Решение: в ОДНОЙ транзакции БД пишем и бизнес-данные, и строку в таблицу
 * outbox. Отдельный процесс (Debezium CDC читает WAL) публикует строки
 * outbox в Kafka. Атомарность обеспечивает сама БД.
 *
 * Задачи:
 *   [ ] 1. Схема таблицы outbox (см. 06-kafka-connect/sql/init.sql).
 *   [ ] 2. Сервис: INSERT в orders + INSERT в outbox в одной транзакции.
 *   [ ] 3. Debezium-коннектор с Outbox Event Router SMT.
 *   [ ] 4. Убить сервис между коммитом БД и публикацией -> доказать,
 *          что событие всё равно доедет до Kafka.
 *
 * Замечание: outbox даёт at-least-once на выходе, не exactly-once.
 * Потребитель обязан быть идемпотентным (см. IdempotentConsumer).
 */
public class OutboxPattern {

    public static void main(String[] args) {
        throw new UnsupportedOperationException("ЭТАП 11: реализуйте самостоятельно");
    }
}
