package dev.kafkalearn.producers;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ЭТАП 2, задача 3: кастомный партиционер (ЗАГОТОВКА - допишите TODO).
 * <p>
 * Идея: VIP-пользователей отправлять в выделенную партицию 0,
 * остальных распределять хешем по оставшимся партициям.
 * Так на партиции 0 можно поставить отдельного консьюмера с приоритетом.
 * <p>
 * Подключение:
 * props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
 * <p>
 * Вопрос к NOTES.md: какие проблемы создаёт такая схема при увеличении
 * числа партиций и при неравномерном распределении VIP-трафика?
 */
public class CustomPartitioner implements Partitioner {

    private static final String VIP_PREFIX_DEFAULT = "vip-";
    private static final int VIP_PARTITION = 0;
    private final ConcurrentHashMap<String, AtomicInteger> topicToPartIdx = new ConcurrentHashMap<>();
    private String vipPrefix = VIP_PREFIX_DEFAULT;

    @Override
    public void configure(Map<String, ?> configs) {
        this.vipPrefix = Optional.ofNullable((String) configs.get("custom.partitioner.vip.prefix"))
            .orElse(VIP_PREFIX_DEFAULT);
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes,
                         Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        if (numPartitions == 1) {
            return 0;
        }

        if (keyBytes == null) {
            return getNextPartIdx(topic, numPartitions);
        }

        String k = key.toString();
        if (k.startsWith(vipPrefix)) {
            return VIP_PARTITION;
        }

        int hash = Utils.toPositive(Utils.murmur2(keyBytes));
        return (hash % (numPartitions - 1)) + 1;
    }

    private int getNextPartIdx(String topic, int numPartitions) {
        AtomicInteger idx = topicToPartIdx.computeIfAbsent(topic, key -> new AtomicInteger(1));
        return (Utils.toPositive(idx.getAndIncrement()) % (numPartitions - 1)) + 1;
    }

    @Override
    public void close() {
    }
}
