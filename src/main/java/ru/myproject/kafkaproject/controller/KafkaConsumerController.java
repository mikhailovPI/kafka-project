package ru.myproject.kafkaproject.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.myproject.kafkaproject.service.ConsumerService;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api")
public class KafkaConsumerController {

    private final ConsumerService consumerService;

    @PostMapping("/consume/single")
    public ResponseEntity<?> singleConsume(@RequestParam String topic) {
        consumerService.singleConsumeMessage(topic);
        return ResponseEntity.accepted().body(Map.of("singleRunning", true, "topic", topic));
    }

    @PostMapping("/consume/batch")
    public ResponseEntity<?> batchConsume(@RequestParam String topic) {
        consumerService.batchConsumeMessage(topic);
        return ResponseEntity.accepted().body(Map.of("batchRunning", true, "topic", topic));
    }

    @PostMapping("/consume/stop")
    public ResponseEntity<?> stop() {
        consumerService.stopAll();
        return ResponseEntity.ok(Map.of("stopped", true));
    }
}