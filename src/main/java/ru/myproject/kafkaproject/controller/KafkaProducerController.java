package ru.myproject.kafkaproject.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.myproject.kafkaproject.model.Message;
import ru.myproject.kafkaproject.service.ProducerService;

import java.util.Map;
import java.util.concurrent.ExecutionException;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api")
public class KafkaProducerController {

    private final ProducerService producer;

    @PostMapping("/send")
    public ResponseEntity<?> send(@RequestBody @Valid Message body) throws ExecutionException, InterruptedException {
        var md = producer.publish(body).get();
        return ResponseEntity.ok(
                Map.of(
                        "topic", md.topic(),
                        "partition", md.partition(),
                        "offset", md.offset())
        );
    }
}
