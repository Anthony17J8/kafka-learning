package dev.kafkalearn.consumers;

import dev.kafkalearn.common.KafkaConfig;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * ЭТАП 3, задача 6: считаем consumer lag через AdminClient.
 * <p>
 * Lag = (last offset в партиции) - (закоммиченный offset группы).
 * Это метрика №1 для здоровья потребления: растёт -> консьюмер не успевает.
 * <p>
 * Запуск:
 * mvn -pl 03-consumers exec:java \
 * -Dexec.mainClass=dev.kafkalearn.consumers.LagMonitor \
 * -Dexec.args="orders-manual-commit"
 * <p>
 * Сравните вывод с:
 * ./scripts/kafka.sh kafka-consumer-groups.sh --describe --group orders-manual-commit
 * <p>
 */
public class LagMonitor {

    private static final Logger log = LoggerFactory.getLogger(LagMonitor.class);
    public static final int POLL_INTERVAL_SEC = 5;
    public static final int POLL_INTERVAL_MS = POLL_INTERVAL_SEC * 1000;

    public static void main(String[] args) throws Exception {
        String groupId = args.length > 0 ? args[0] : "orders-manual-commit";

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());

        try (Admin admin = Admin.create(props)) {
            LagSnapshot previous = null;
            int iterNumber = 0;
            while (true) {
                Optional<LagSnapshot> ls = takeLagSnapshot(admin, groupId);
                if (ls.isEmpty()) {
                    Thread.sleep(POLL_INTERVAL_MS);
                    continue;
                }
                LagSnapshot lagSnapshot = ls.get();
                if (previous != null) {
                    long delta = lagSnapshot.totalLag() - previous.totalLag();
                    double speed = Math.abs((1.0 * delta) / POLL_INTERVAL_SEC);
                    String result;
                    if (delta < 0) {
                        double time = lagSnapshot.totalLag() / speed;
                        result = "консьюмер догонит продюсер через %.2f сек".formatted(time);
                    } else if (delta > 0) {
                        result = "lag растет, консьюмер не успевает";
                    } else {
                        result = "lag не меняется, либо продюсер/консьюмер молчит, либо консьюмер успевает прочитывать все записи";
                    }
                    System.out.printf(
                        "lag=%d, Δ=%+d, %s\n", lagSnapshot.totalLag(), delta, result);
                }
                previous = lagSnapshot;

                if (iterNumber % 12 == 0) {
                    print(lagSnapshot);
                }
                iterNumber++;
                Thread.sleep(POLL_INTERVAL_MS);
            }
        }
    }

    private static void print(LagSnapshot lagSnapshot) {
        System.out.printf("%n%-30s %6s %12s %12s %8s%n",
            "TOPIC", "PART", "COMMITTED", "END", "LAG");
        System.out.println("-".repeat(72));

        for (Map.Entry<TopicPartition, LagSnapshot.OffsetLagInfo> partEntry : lagSnapshot.partitionLag().entrySet()) {
            LagSnapshot.OffsetLagInfo value = partEntry.getValue();
            System.out.printf("%-30s %6d %12d %12d %8d%n",
                partEntry.getKey().topic(), partEntry.getKey().partition(),
                value.committed(), value.endOffset(), value.lag());
        }


        System.out.println("-".repeat(72));
        System.out.printf("%-30s %6s %12s %12s %8d%n", "ИТОГО", "", "", "", lagSnapshot.totalLag());
    }

    private static Optional<LagSnapshot> takeLagSnapshot(Admin admin, String groupId)
        throws InterruptedException, ExecutionException {
        // 1. Закоммиченные оффсеты группы
        ListConsumerGroupOffsetsResult offsetsResult = admin.listConsumerGroupOffsets(groupId);
        Map<TopicPartition, OffsetAndMetadata> committed =
            offsetsResult.partitionsToOffsetAndMetadata().get();

        if (committed.isEmpty()) {
            log.warn("У группы '{}' нет закоммиченных оффсетов", groupId);
            return Optional.empty();
        }

        // 2. Конец лога (last offset) по тем же партициям
        Map<TopicPartition, OffsetSpec> latestSpec = new HashMap<>();
        committed.keySet().forEach(tp -> latestSpec.put(tp, OffsetSpec.latest()));
        var endOffsets = admin.listOffsets(latestSpec).all().get();

        long totalLag = 0;
        Map<TopicPartition, LagSnapshot.OffsetLagInfo> partitionLag = new HashMap<>();
        for (var entry : committed.entrySet()) {
            TopicPartition tp = entry.getKey();
            long committedOffset = entry.getValue().offset();
            ListOffsetsResult.ListOffsetsResultInfo lori = endOffsets.get(tp);
            if (lori == null) {
                log.info("Нет информации по партиции {}:{}", tp.topic(), tp.partition());
                continue;
            }
            long endOffset = lori.offset();
            long lag = endOffset - committedOffset;
            partitionLag.put(tp, new LagSnapshot.OffsetLagInfo(committedOffset, endOffset, lag));
            totalLag += lag;
        }
        return Optional.of(new LagSnapshot(totalLag, partitionLag));
    }

    record LagSnapshot(long totalLag, Map<TopicPartition, OffsetLagInfo> partitionLag) {

        record OffsetLagInfo(long committed, long endOffset, long lag) {

        }
    }

}
