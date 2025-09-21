package ru.myproject.kafkaproject.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.stereotype.Service;
import ru.myproject.kafkaproject.model.Message;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProducerService {

    private final KafkaProducer<String, Message> kafkaProducer;

    public CompletableFuture<RecordMetadata> publish(Message message) {
        log.info("[PRODUCER] OUT {}", message);
        ProducerRecord<String, Message> record = new ProducerRecord<>(message.getTopic(), message.getKey(), message);
        CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
        kafkaProducer.send(record, (md, ex) -> {
            if (ex != null) {
                log.error("Send failed", ex);
                completableFuture.completeExceptionally(ex);
            } else {
                log.info("Sent to {}-{} offset={}", md.topic(), md.partition(), md.offset());
                completableFuture.complete(md);
            }
        });
        return completableFuture;
    }
}
