package ru.myproject.kafkaproject.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class MessageDeserializer implements Deserializer<Message> {
    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Message deserialize(String topic, byte[] data) {
        if (data == null) return null;
        try {
            return mapper.readValue(data, Message.class);
        } catch (Exception e) {
            throw new SerializationException("Failed to deserialize Message", e);
        }
    }

    @Override
    public void close() {
    }
}
