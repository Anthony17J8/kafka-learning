package dev.kafkalearn.consumers;

import dev.kafkalearn.common.KafkaConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * ЭТАП 3, задача 6: считаем consumer lag через AdminClient.
 *
 * Lag = (last offset в партиции) - (закоммиченный offset группы).
 * Это метрика №1 для здоровья потребления: растёт -> консьюмер не успевает.
 *
 * Запуск:
 *   mvn -pl 03-consumers exec:java \
 *     -Dexec.mainClass=dev.kafkalearn.consumers.LagMonitor \
 *     -Dexec.args="orders-manual-commit"
 *
 * Сравните вывод с:
 *   ./scripts/kafka.sh kafka-consumer-groups.sh --describe --group orders-manual-commit
 *
 * TODO: сделать цикл с интервалом 5 сек и печатать дельту lag,
 *       чтобы видеть, догоняет ли консьюмер продюсера.
 */
public class LagMonitor {

    private static final Logger log = LoggerFactory.getLogger(LagMonitor.class);

    public static void main(String[] args) throws Exception {
        String groupId = args.length > 0 ? args[0] : "orders-manual-commit";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());

        try (Admin admin = Admin.create(props)) {
            // 1. Закоммиченные оффсеты группы
            ListConsumerGroupOffsetsResult offsetsResult = admin.listConsumerGroupOffsets(groupId);
            Map<TopicPartition, OffsetAndMetadata> committed =
                    offsetsResult.partitionsToOffsetAndMetadata().get();

            if (committed.isEmpty()) {
                log.warn("У группы '{}' нет закоммиченных оффсетов", groupId);
                return;
            }

            // 2. Конец лога (last offset) по тем же партициям
            Map<TopicPartition, OffsetSpec> latestSpec = new HashMap<>();
            committed.keySet().forEach(tp -> latestSpec.put(tp, OffsetSpec.latest()));
            var endOffsets = admin.listOffsets(latestSpec).all().get();

            System.out.printf("%n%-30s %6s %12s %12s %8s%n",
                    "TOPIC", "PART", "COMMITTED", "END", "LAG");
            System.out.println("-".repeat(72));

            long totalLag = 0;
            for (var entry : committed.entrySet()) {
                TopicPartition tp = entry.getKey();
                long committedOffset = entry.getValue().offset();
                long endOffset = endOffsets.get(tp).offset();
                long lag = endOffset - committedOffset;
                totalLag += lag;

                System.out.printf("%-30s %6d %12d %12d %8d%n",
                        tp.topic(), tp.partition(), committedOffset, endOffset, lag);
            }
            System.out.println("-".repeat(72));
            System.out.printf("%-30s %6s %12s %12s %8d%n", "ИТОГО", "", "", "", totalLag);
        }
    }
}
