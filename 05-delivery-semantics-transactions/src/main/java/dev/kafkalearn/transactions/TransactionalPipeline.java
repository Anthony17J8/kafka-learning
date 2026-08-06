package dev.kafkalearn.transactions;

/**
 * ЭТАП 5 — ЗАГОТОВКА. Реализуйте паттерн consume-transform-produce с EOS.
 *
 * Каркас:
 *   1. Producer с TRANSACTIONAL_ID_CONFIG (уникальный и СТАБИЛЬНЫЙ между рестартами!)
 *      + ENABLE_IDEMPOTENCE_CONFIG=true.
 *   2. Consumer с ISOLATION_LEVEL_CONFIG="read_committed"
 *      и ENABLE_AUTO_COMMIT_CONFIG=false.
 *   3. producer.initTransactions() один раз при старте.
 *   4. Цикл:
 *        beginTransaction()
 *        -> обработать батч, отправить результат в выходной топик
 *        -> producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())
 *        -> commitTransaction()
 *      При исключении: abortTransaction().
 *
 * Задачи этапа:
 *   [ ] 1. Атомарная запись в ДВА топика (orders-enriched + metrics).
 *   [ ] 2. Оффсеты коммитятся внутри транзакции.
 *   [ ] 3. Убить процесс между send и commit -> доказать отсутствие
 *          частичных записей у read_committed консьюмера.
 *   [ ] 4. Сравнить выдачу read_committed vs read_uncommitted.
 *
 * Ловушка: transactional.id должен быть стабильным для инстанса, иначе
 * zombie fencing не сработает. Что произойдёт, если сгенерировать UUID
 * при каждом старте? Ответ — в NOTES.md.
 */
public class TransactionalPipeline {

    public static void main(String[] args) {
        throw new UnsupportedOperationException("ЭТАП 5: реализуйте самостоятельно");
    }
}
