package dev.kafkalearn.consumers;

import dev.kafkalearn.common.JsonSerde;
import dev.kafkalearn.common.KafkaConfig;
import dev.kafkalearn.common.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ЭТАП 3, задача 5: чтение с конкретной позиции.
 *
 * Демонстрирует ручное назначение партиций (assign) вместо subscribe:
 * без группы, без ребалансировки, полный контроль над позицией.
 * Типовой кейс: разбор инцидента - "перечитать всё за последний час".
 *
 * Запуск:
 *   mvn -pl 03-consumers exec:java \
 *     -Dexec.mainClass=dev.kafkalearn.consumers.SeekingConsumer
 */
public class SeekingConsumer {

    private static final Logger log = LoggerFactory.getLogger(SeekingConsumer.class);
    private static final String TOPIC = KafkaConfig.topic("orders");
    private static final Duration LOOKBACK = Duration.ofHours(1);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // GROUP_ID не нужен: мы не участвуем в группе

        try (KafkaConsumer<String, OrderEvent> consumer = new KafkaConsumer<>(
                props, new StringDeserializer(), JsonSerde.deserializer(OrderEvent.class))) {

            // Ручное назначение всех партиций топика
            List<TopicPartition> partitions = new ArrayList<>();
            for (PartitionInfo pi : consumer.partitionsFor(TOPIC)) {
                partitions.add(new TopicPartition(TOPIC, pi.partition()));
            }
            consumer.assign(partitions);
            log.info("Назначены партиции: {}", partitions);

            // Ищем оффсеты по времени
            long targetTs = Instant.now().minus(Duration.ofDays(7)).toEpochMilli();
            Map<TopicPartition, Long> query = new HashMap<>();
            partitions.forEach(tp -> query.put(tp, targetTs));

            Map<TopicPartition, OffsetAndTimestamp> found = consumer.offsetsForTimes(query);

            found.forEach((tp, offsetAndTs) -> {
                if (offsetAndTs == null) {
                    // Записей позднее targetTs нет -> идём в конец
                    log.info("{}: записей за период нет, seekToEnd", tp);
                    consumer.seekToEnd(List.of(tp));
                } else {
                    log.info("{}: seek to offset={} (ts={})",
                            tp, offsetAndTs.offset(), Instant.ofEpochMilli(offsetAndTs.timestamp()));
                    consumer.seek(tp, offsetAndTs.offset());
                }
            });

            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofSeconds(2));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                for (ConsumerRecord<String, OrderEvent> r : records) {
                    log.info("p={} off={} ts={} key={}",
                            r.partition(), r.offset(), Instant.ofEpochMilli(r.timestamp()), r.key());
                }
            }
            log.info("Чтение завершено");
        }
    }
}
