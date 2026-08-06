package dev.kafkalearn.producers;

import dev.kafkalearn.common.JsonSerde;
import dev.kafkalearn.common.KafkaConfig;
import dev.kafkalearn.common.OrderEvent;
import dev.kafkalearn.common.OrderGenerator;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

/**
 * ЭТАП 2, задачи 2 и 4: сравнение конфигураций по throughput и latency.
 * <p>
 * Прогоняет один и тот же объём сообщений при разных наборах настроек
 * и печатает сводную таблицу. Результаты вставьте в NOTES.md.
 * <p>
 * Запуск:
 * mvn -pl 02-producers exec:java \
 * -Dexec.mainClass=dev.kafkalearn.producers.ThroughputBenchmark
 * <p>
 * TODO для вас:
 *  - добавить замер p99 latency (сейчас считается только средняя);
 *  - прогнать против кластера из 3 брокеров и сравнить с single;
 *  - добавить вариант с compression.type=zstd и сравнить с lz4.
 */
public class ThroughputBenchmark {

    private static final Logger log = LoggerFactory.getLogger(ThroughputBenchmark.class);
    private static final String TOPIC = KafkaConfig.topic("orders");
    private static final int MESSAGES = 20_000;

    record Scenario(String name, String acks, int lingerMs, int batchSize, String compression) {
    }

    public static void main(String[] args) {
        Scenario[] scenarios = {
            new Scenario("acks=0, без батчинга", "0", 0, 16 * 1024, "none"),
            new Scenario("acks=1, без батчинга", "1", 0, 16 * 1024, "none"),
            new Scenario("acks=all, без батчинга", "all", 0, 16 * 1024, "none"),
            new Scenario("acks=all, linger=20", "all", 20, 64 * 1024, "none"),
            new Scenario("acks=all, linger=20+lz4", "all", 20, 64 * 1024, "lz4"),
            new Scenario("acks=all, linger=20+zstd", "all", 20, 64 * 1024, "lz4"),
        };

        System.out.printf("%n%-28s %10s %14s %12s%n", "СЦЕНАРИЙ", "мс", "msg/sec", "avg lat, мс");
        System.out.println("-".repeat(68));

        run(new Scenario("warmup", "1", 0, 16 * 1024, "none"));
        for (Scenario s : scenarios) {
            Result r = run(s);
            System.out.printf("%-28s %10d %14.0f %12.2f%n",
                s.name(), r.elapsedMs, r.throughput, r.avgLatencyMs);
        }
    }

    record Result(long elapsedMs, double throughput, double avgLatencyMs) {
    }

    private static Result run(Scenario s) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, s.acks());
        props.put(ProducerConfig.LINGER_MS_CONFIG, s.lingerMs());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, s.batchSize());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, s.compression());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "all".equals(s.acks()));

        AtomicLong totalLatency = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        long start = System.currentTimeMillis();

        try (Producer<String, OrderEvent> producer = new KafkaProducer<>(
            props, new StringSerializer(), JsonSerde.serializer())) {
            for (int i = 0; i < MESSAGES; i++) {
                OrderEvent order = OrderGenerator.next();
                long sentAt = System.nanoTime();
                producer.send(new ProducerRecord<>(TOPIC, order.userId(), order),
                    (md, ex) -> {
                        if (ex != null) {
                            errors.incrementAndGet();
                        } else {
                            totalLatency.addAndGet(System.nanoTime() - sentAt);
                        }
                    });
            }
            producer.flush();
            producer.metrics()
                .forEach(
                    ((metricName, metric) -> {
                        if (metricName.name().equals("compression-rate-avg")) {
                            System.out.printf("Metrics: %s = %s\n", metric.metricName().name(), metric.metricValue());
                        }
                    })
                );
        }

        long elapsed = Math.max(1, System.currentTimeMillis() - start);
        if (errors.get() > 0) {
            log.warn("Ошибок при отправке: {}", errors.get());
        }
        return new Result(
            elapsed,
            MESSAGES * 1000.0 / elapsed,
            totalLatency.get() / 1_000_000.0 / MESSAGES
        );
    }
}
