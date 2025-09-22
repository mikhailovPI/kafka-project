package ru.myproject.kafkaproject.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.message.DescribeProducersRequestData;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import ru.myproject.kafkaproject.model.TopicRequest;
import ru.myproject.kafkaproject.service.ConsumerService;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api")
public class KafkaConsumerController {

    private final ConsumerService consumerService;

    @PostMapping("/consume/single")
    public ResponseEntity<?> singleConsume(@RequestBody @Valid TopicRequest topic) {
        consumerService.singleConsumeMessage(topic);
        return ResponseEntity.accepted().body(Map.of("singleRunning", true, "topic", topic));
    }

    @PostMapping("/consume/batch")
    public ResponseEntity<?> batchConsume(@RequestBody @Valid TopicRequest topic) {
        consumerService.batchConsumeMessage(topic);
        return ResponseEntity.accepted().body(Map.of("batchRunning", true, "topic", topic));
    }

    @PostMapping("/consume/stop")
    public ResponseEntity<?> stop() {
        consumerService.stopAll();
        return ResponseEntity.ok(Map.of("stopped", true));
    }
}