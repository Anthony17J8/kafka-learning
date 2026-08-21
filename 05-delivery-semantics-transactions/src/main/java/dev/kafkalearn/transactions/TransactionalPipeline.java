package dev.kafkalearn.transactions;

import dev.kafkalearn.common.JsonSerde;
import dev.kafkalearn.common.KafkaConfig;
import dev.kafkalearn.common.OrderEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

/**
 * ЭТАП 5 — ЗАГОТОВКА. Реализуйте паттерн consume-transform-produce с EOS.
 * <p>
 * Каркас:
 * 1. Producer с TRANSACTIONAL_ID_CONFIG (уникальный и СТАБИЛЬНЫЙ между рестартами!)
 * + ENABLE_IDEMPOTENCE_CONFIG=true.
 * 2. Consumer с ISOLATION_LEVEL_CONFIG="read_committed"
 * и ENABLE_AUTO_COMMIT_CONFIG=false.
 * 3. producer.initTransactions() один раз при старте.
 * 4. Цикл:
 * beginTransaction()
 * -> обработать батч, отправить результат в выходной топик
 * -> producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())
 * -> commitTransaction()
 * При исключении: abortTransaction().
 * <p>
 * Задачи этапа:
 * [ ] 1. Атомарная запись в ДВА топика (orders-enriched + metrics).
 * [ ] 2. Оффсеты коммитятся внутри транзакции.
 * [ ] 3. Убить процесс между send и commit -> доказать отсутствие
 * частичных записей у read_committed консьюмера.
 * [ ] 4. Сравнить выдачу read_committed vs read_uncommitted.
 * <p>
 * Ловушка: transactional.id должен быть стабильным для инстанса, иначе
 * zombie fencing не сработает. Что произойдёт, если сгенерировать UUID
 * при каждом старте? Ответ — в NOTES.md.
 */
public class TransactionalPipeline {

    private static final String INPUT_TOPIC = "tx-input";
    private static final String OUTPUT_TOPIC_A = "tx-output-a";
    private static final String OUTPUT_TOPIC_B = "tx-output-b";
    private static final int MESSAGES = 10;
    private static final Logger log = LoggerFactory.getLogger(TransactionalPipeline.class);

    public static void main(String[] args) {
        boolean isCommitMode = !Boolean.parseBoolean(System.getProperty("abort", "false"));
        Properties props = producerProps();
        try (KafkaProducer<String, OrderEvent> txProducer = new KafkaProducer<>(props,
            new StringSerializer(),
            JsonSerde.serializer())) {
            int num = 0;
            txProducer.initTransactions();
            txProducer.beginTransaction();
            while (num < MESSAGES) {
                OrderEvent order = new OrderEvent(
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    "Shampoo",
                    10,
                    1.0,
                    OrderEvent.Status.CREATED,
                    Instant.now());
                String key = "order-" + (num % 5);
                sendTo(txProducer, OUTPUT_TOPIC_A, key, order);
                sendTo(txProducer, OUTPUT_TOPIC_B, key, order);
                num++;

            }
            txProducer.flush();
            if (isCommitMode) {
                txProducer.commitTransaction();
            } else {
                txProducer.abortTransaction();
            }
        }
    }

    private static void sendTo(KafkaProducer<String, OrderEvent> txProducer,
                               String topic,
                               String key,
                               OrderEvent order) {
        txProducer.send(new ProducerRecord<>(topic, key, order),
            ((metadata, exception) -> {
                if (exception != null) {
                    log.error("Producer has error during send", exception);
                } else {
                    log.info("Message has been sent: topic={}, partition={}, offset={}",
                        metadata.topic(), metadata.partition(), metadata.offset());
                }
            }));
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());
        String txId = System.getProperty("transactional.id", "transactional-pipeline-1");
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, txId);
        return props;
    }
}
