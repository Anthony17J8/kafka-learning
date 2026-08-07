package dev.kafkalearn.producers;

import dev.kafkalearn.common.KafkaConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class OrderingDemoProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderingDemoProducer.class);
    private static final String TOPIC = KafkaConfig.topic("ordering-test");
    private static final int MESSAGES = 150_000;

    public static void main(String[] args) {
        Properties props = producerProps();
        log.info("Старт отправки!");
        long asyncStart = System.currentTimeMillis();
        try (Producer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(),
            new StringSerializer())) {
            for (int i = 0; i < MESSAGES; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, "key", String.valueOf(i));

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        // Сюда попадают неретраебельные ошибки и исчерпанные ретраи
                        log.error("Ошибка отправки", exception);
                    }
                });
            }
            // flush() выталкивает всё, что скопилось в батчах
            producer.flush();
            log.info("Асинхронно за {} мс", System.currentTimeMillis() - asyncStart);
        }
    }

    static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ordering-producer");

        boolean idempotenceEnabled = Boolean.parseBoolean(System.getProperty("idempotence", "true"));
        props.put(ProducerConfig.ACKS_CONFIG, idempotenceEnabled ? "all" : "1");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, idempotenceEnabled);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000);
        props.put(ProducerConfig.RETRIES_CONFIG, 10);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1024);

        return props;
    }
}
