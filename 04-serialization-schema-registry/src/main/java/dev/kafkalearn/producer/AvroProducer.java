package dev.kafkalearn.producer;

import dev.kafkalearn.avro.OrderEvent;
import dev.kafkalearn.avro.Status;
import dev.kafkalearn.common.KafkaConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

public class AvroProducer {
    private static final String MESSAGES_TOTAL = "20";
    private static final String TOPIC = "orders-avro";
    private static final Logger log = LoggerFactory.getLogger(AvroProducer.class);

    public static void main(String[] args) {
        Properties props = producerProps();
        int messages = Integer.parseInt(System.getProperty("message.total", MESSAGES_TOTAL));
        try (KafkaProducer<String, OrderEvent> producer = new KafkaProducer<>(props)) {
            int cnt = 0;
            while (cnt < messages) {
                producer.send(new ProducerRecord<>(TOPIC, "order-" + (cnt % 5),
                    OrderEvent.newBuilder()
                        .setUserId(UUID.randomUUID().toString())
                        .setCreatedAt(Instant.now())
                        .setOrderId(UUID.randomUUID().toString())
                        .setPrice(1.0)
                        .setProduct("Shampoo")
                        .setQuantity(10)
                        .setStatus(Status.CREATED)
                        .build()), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Producer has error during send", exception);
                    } else {
                        log.info("Message has been sent: topic={}, partition={}, offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
                cnt++;
            }
            producer.flush();
        }
    }

    private static Properties producerProps() {
        Properties props = new Properties();
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "avro-producer");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("auto.register.schemas", true);
        return props;
    }
}
