package dev.kafkalearn.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Простейшая JSON-сериализация на Jackson.
 *
 * ВАЖНО (тема этапа 4): такой подход не даёт контроля эволюции схемы.
 * На этапе 4 мы заменим его на Avro + Schema Registry и увидим разницу.
 */
public final class JsonSerde {

    public static final ObjectMapper MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private JsonSerde() {
    }

    public static <T> Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new SerializationException("Не удалось сериализовать в JSON", e);
            }
        };
    }

    public static <T> Deserializer<T> deserializer(Class<T> type) {
        return (topic, bytes) -> {
            if (bytes == null) {
                return null;
            }
            try {
                return MAPPER.readValue(bytes, type);
            } catch (Exception e) {
                throw new SerializationException("Не удалось десериализовать JSON", e);
            }
        };
    }
}
