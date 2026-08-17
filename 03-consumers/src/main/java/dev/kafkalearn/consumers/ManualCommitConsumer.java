package dev.kafkalearn.consumers;

import dev.kafkalearn.common.JsonSerde;
import dev.kafkalearn.common.KafkaConfig;
import dev.kafkalearn.common.OrderEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ЭТАП 3, задачи 1, 2, 4: консьюмер с ручным коммитом и rebalance listener.
 * <p>
 * Запуск (несколько терминалов = несколько инстансов одной группы):
 * mvn -pl 03-consumers -am compile
 * mvn -pl 03-consumers exec:java -Dexec.mainClass=dev.kafkalearn.consumers.ManualCommitConsumer
 * <p>
 * Что наблюдать:
 * - запустите 1, 2, 3, 4 инстанса на топике с 3 партициями;
 * - при 4-м инстансе один останется без партиций (партиций меньше, чем консьюмеров);
 * - убейте один инстанс -> в логах увидите revoke/assign, то есть ребалансировку.
 * <p>
 * Ключевая идея: коммитим ПОСЛЕ обработки => семантика at-least-once.
 * Обработка должна быть идемпотентной (см. этап 11).
 */
public class ManualCommitConsumer {

    private static final Logger log = LoggerFactory.getLogger(ManualCommitConsumer.class);
    private static final String TOPIC = KafkaConfig.topic("orders");
    private static final String GROUP_ID = "orders-manual-commit";

    public static void main(String[] args) {
        Properties props = consumerProps();

        KafkaConsumer<String, OrderEvent> consumer = new KafkaConsumer<>(
            props, new StringDeserializer(), JsonSerde.deserializer(OrderEvent.class));

        // Позиции, которые уже обработаны, но ещё не закоммичены
        Map<TopicPartition, OffsetAndMetadata> pending = new HashMap<>();

        // Корректное завершение по Ctrl+C: wakeup() прерывает poll()
        Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Получен сигнал завершения, будим consumer...");
            consumer.wakeup();
            try {
                main.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        String topic = System.getProperty("topic", TOPIC);
        try {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                private long revokeTime = -1L;
                private long assignTime = -1L;

                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    // Критично: коммитим до того, как потеряем партиции,
                    // иначе новый владелец перечитает уже обработанное.
                    log.warn("REVOKED: {}", partitions);
                    revokeTime = System.currentTimeMillis();

                    if (!pending.isEmpty()) {
                        consumer.commitSync(pending);
                        pending.clear();
                    }
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    assignTime = System.currentTimeMillis();
                    log.info("ASSIGNED: {}", partitions);
                    if (revokeTime != -1L) {
                        log.info("REASSIGN completed in {} ms", assignTime - revokeTime);
                        revokeTime = -1L;
                    }
                }

                @Override
                public void onPartitionsLost(Collection<TopicPartition> partitions) {
                    // Партиции отобраны принудительно (например, вылет из группы).
                    // Коммитить уже нельзя - владелец сменился.
                    log.error("LOST: {}", partitions);
                    revokeTime = -1L;
                    pending.clear();
                }
            });

            while (true) {
                ConsumerRecords<String, OrderEvent> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, OrderEvent> record : records) {
                    process(record);
                    pending.put(
                        new TopicPartition(record.topic(), record.partition()),
                        // Коммитим offset + 1: это позиция СЛЕДУЮЩЕЙ записи
                        new OffsetAndMetadata(record.offset() + 1)
                    );
                }

                if (!pending.isEmpty()) {
                    // Асинхронный коммит в горячем цикле - быстрее, но без гарантии.
                    consumer.commitAsync(pending, (offsets, ex) -> {
                        if (ex != null) {
                            log.warn("Асинхронный коммит не удался: {}", ex.getMessage());
                        }
                    });
                }
            }
        } catch (WakeupException e) {
            log.info("Штатное завершение");
        } finally {
            try {
                // Финальный синхронный коммит: здесь нам важна гарантия
                if (!pending.isEmpty()) {
                    consumer.commitSync(pending);
                }
            } finally {
                consumer.close();
                log.info("Consumer закрыт");
            }
        }
    }

    private static void process(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent order = record.value();
        log.info("p={} off={} key={} order={} total={}",
            record.partition(), record.offset(), record.key(),
            order.orderId(), String.format("%.2f", order.total()));
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    static Properties consumerProps() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());

        String groupId = System.getProperty("group.id", GROUP_ID);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // Ручной коммит - основа контроля над семантикой доставки
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,  5_000);

        String grProtocol = System.getProperty("group.protocol", "classic");
        if ("classic".equals(grProtocol)) {
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45_000);
            props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3_000);
        }
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, grProtocol);

        return props;
    }
}
