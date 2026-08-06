package dev.kafkalearn.common;

/**
 * Единая точка правды по адресам окружения.
 * Всё переопределяется переменными окружения или -D системными свойствами,
 * чтобы один и тот же код работал и против single-broker, и против кластера.
 *
 * Примеры:
 *   BOOTSTRAP_SERVERS=localhost:19092,localhost:29092,localhost:39092
 *   java -DbootstrapServers=localhost:9092 ...
 */
public final class KafkaConfig {

    private KafkaConfig() {
    }

    /** Одиночный брокер (docker/single-broker.yml). */
    public static final String SINGLE = "localhost:9092";

    /** Кластер из трёх нод (docker/three-broker.yml, docker/full-stack.yml). */
    public static final String CLUSTER = "localhost:19092,localhost:29092,localhost:39092";

    public static String bootstrapServers() {
        return resolve("BOOTSTRAP_SERVERS", "bootstrapServers", SINGLE);
    }

    public static String schemaRegistryUrl() {
        return resolve("SCHEMA_REGISTRY_URL", "schemaRegistryUrl", "http://localhost:8081");
    }

    public static String connectUrl() {
        return resolve("CONNECT_URL", "connectUrl", "http://localhost:8083");
    }

    /** Топик по умолчанию, можно переопределить для конкретного запуска. */
    public static String topic(String defaultTopic) {
        return resolve("TOPIC", "topic", defaultTopic);
    }

    private static String resolve(String envKey, String propKey, String fallback) {
        String fromProp = System.getProperty(propKey);
        if (fromProp != null && !fromProp.isBlank()) {
            return fromProp;
        }
        String fromEnv = System.getenv(envKey);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv;
        }
        return fallback;
    }
}
