package com.example.kafkademo;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

/**
 * Minimal JSON serializer/deserializer helpers for Kafka.
 * Keeping this in one file to reduce boilerplate in the demo.
 */
public final class JsonSerDeser {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonSerDeser() {}

    public static <T> Serializer<T> serializer() {
        return (topic, data) -> {
            if (data == null) return null;
            try {
                return MAPPER.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Failed to serialize " + data, e);
            }
        };
    }

    public static <T> Deserializer<T> deserializer(Class<T> type) {
        return (topic, bytes) -> {
            if (bytes == null) return null;
            try {
                return MAPPER.readValue(new String(bytes, StandardCharsets.UTF_8), type);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize message on " + topic, e);
            }
        };
    }
}
