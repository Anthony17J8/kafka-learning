package dev.kafkalearn.producers;

import dev.kafkalearn.common.JsonSerde;
import dev.kafkalearn.common.KafkaConfig;
import dev.kafkalearn.common.OrderEvent;
import dev.kafkalearn.common.OrderGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * ЭТАП 2, задача 1: продюсер с синхронной и асинхронной отправкой.
 * <p>
 * Запуск:
 * mvn -pl 02-producers -am compile
 * mvn -pl 02-producers exec:java -Dexec.mainClass=dev.kafkalearn.producers.SimpleProducer
 * <p>
 * Против кластера:
 * BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092 mvn -pl 02-producers exec:java ...
 * <p>
 * Что посмотреть:
 * - лог показывает partition и offset каждой записи;
 * - записи с одинаковым ключом (userId) всегда попадают в одну партицию;
 * - синхронная отправка на порядок медленнее асинхронной.
 */
public class SimpleProducer {

    private static final Logger log = LoggerFactory.getLogger(SimpleProducer.class);
    private static final String TOPIC = KafkaConfig.topic("orders");
    private static final int MESSAGES = 100_000;

    public static void main(String[] args) throws Exception {
        Properties props = producerProps();

        try (Producer<String, OrderEvent> producer = new KafkaProducer<>(
            props, new StringSerializer(), JsonSerde.serializer())) {

            log.info("=== Синхронная отправка ({} сообщений) ===", MESSAGES / 2);
            long syncStart = System.currentTimeMillis();
            for (int i = 0; i < MESSAGES / 2; i++) {
                OrderEvent order = OrderGenerator.next();
                // Ключ = userId: все заказы одного пользователя идут в одну партицию
                ProducerRecord<String, OrderEvent> record =
                    new ProducerRecord<>(TOPIC, order.userId(), order);

                // .get() блокирует поток до подтверждения от брокера
                RecordMetadata md = producer.send(record).get();
                log.info("sync  -> key={} partition={} offset={} timestamp={}",
                    order.userId(), md.partition(), md.offset(), md.timestamp());
            }
            log.info("Синхронно за {} мс", System.currentTimeMillis() - syncStart);

            log.info("=== Асинхронная отправка ({} сообщений) ===", MESSAGES / 2);
            long asyncStart = System.currentTimeMillis();
            CountDownLatch latch = new CountDownLatch(MESSAGES / 2);
            for (int i = 0; i < MESSAGES / 2; i++) {
                OrderEvent order = OrderGenerator.next();
                ProducerRecord<String, OrderEvent> record =
                    new ProducerRecord<>(TOPIC, order.userId(), order);

                producer.send(record, (metadata, exception) -> {
                    try {
                        if (exception != null) {
                            // Сюда попадают неретраебельные ошибки и исчерпанные ретраи
                            log.error("Ошибка отправки", exception);
                        } else {
                            log.info("async -> partition={} offset={}",
                                metadata.partition(), metadata.offset());
                        }
                    } finally {
                        latch.countDown();
                    }
                });
            }
            // flush() выталкивает всё, что скопилось в батчах
            producer.flush();
            latch.await();
            log.info("Асинхронно за {} мс", System.currentTimeMillis() - asyncStart);
        }
    }

    /**
     * Конфигурация продюсера. Каждая опция — предмет отдельного эксперимента
     * (см. задачи 2 и 4 этапа 2).
     */
    static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "simple-producer");

        // --- Надёжность ---
        // acks=all: лидер ждёт подтверждения от всех in-sync реплик.
        // В связке с min.insync.replicas=2 это защита от потери данных.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Идемпотентность: защита от дублей при ретраях (в 4.x включена по умолчанию).
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30_000);

        // --- Пропускная способность ---
        // linger.ms>0 даёт продюсеру время накопить батч. Задержка в обмен на throughput.
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 32 * 1024 * 1024);

        return props;
    }
}
