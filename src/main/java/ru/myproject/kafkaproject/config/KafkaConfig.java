package ru.myproject.kafkaproject.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.myproject.kafkaproject.model.Message;
import ru.myproject.kafkaproject.model.MessageDeserializer;
import ru.myproject.kafkaproject.model.MessageSerializer;

import java.util.Properties;

@Configuration
public class KafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    public String bootstrapService;

    @Value("${spring.kafka.producer.acks:all}")
    public String acks;

    @Value("${spring.kafka.consumer.max.poll.records:500}")
    public int maxPollRecords;
    @Value("${spring.kafka.consumer.fetch.min.size:1024}")
    public int fetchMinBytes;
    @Value("${spring.kafka.consumer.fetch.max.wait:200}")
    public int fetchMaxWaitMs;
    @Value("${spring.kafka.consumer.auto.offset.reset}")
    public String autoOffsetReset;

    @Bean(destroyMethod = "close")
    public KafkaProducer<String, Message> kafkaProducer() {
        return new KafkaProducer<>(producerProps());
    }

    public Properties producerProps() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapService);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, acks);
        properties.put(ProducerConfig.RETRIES_CONFIG, 5);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        return properties;
    }

    public Properties singleConsumerProps() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapService);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "single-consumer-group");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "single-consumer");
        return properties;
    }

    public Properties batchConsumerProps() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapService);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MessageDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "batch-consumer-group");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, fetchMinBytes);
        properties.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, fetchMaxWaitMs);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "batch-consumer");
        return properties;
    }
}
