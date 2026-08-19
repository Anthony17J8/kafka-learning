package dev.kafkalearn.consumer;

import dev.kafkalearn.avro.OrderEvent;
import dev.kafkalearn.common.KafkaConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class AvroConsumer {
    private static final String GROUP_ID = "avro-group";
    private static final Logger log = LoggerFactory.getLogger(AvroConsumer.class);
    private static final String TOPIC = "orders-avro";

    public static void main(String[] args) {
        Properties props = propsConsumer();
        try (KafkaConsumer<String, OrderEvent> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            try {
                int emptyPolls = 0;
                while (emptyPolls < 4) {
                    ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofMillis(500));
                    if (records.isEmpty()) {
                        emptyPolls++;
                        continue;
                    }
                    for (ConsumerRecord<String, OrderEvent> rec : records) {
                        log.info("Consumed record: topic={}, partition={}, key={}, offset={}", rec.topic(),
                            rec.partition(),
                            rec.key(), rec.offset());
                        log.info("OrderEvent: {}", rec.value());
                    }
                }
            } finally {
                consumer.commitSync();
            }
        }
    }

    private static Properties propsConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, System.getProperty("group.id", GROUP_ID));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("schema.registry.url", "http://localhost:8081");
        props.put("specific.avro.reader", true);
        return props;
    }
}
