package ru.myproject.kafkaproject.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Service;
import ru.myproject.kafkaproject.config.KafkaConfig;
import ru.myproject.kafkaproject.model.Message;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ConsumerService {

    public final KafkaConfig kafkaConfig;

    private volatile boolean singleRunning = false;
    private volatile boolean batchRunning = false;

    private Thread singleThread;
    private Thread batchThread;
    private KafkaConsumer<String, Message> singleConsumerRef;
    private KafkaConsumer<String, Message> batchConsumerRef;

    public void singleConsumeMessage(String topic) {
        if (singleRunning) {
            log.info("[SINGLE] already running on topic={}", topic);
            return;
        }
        singleRunning = true;

        singleThread = new Thread(() -> {
            try (KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(kafkaConfig.singleConsumerProps())) {
                singleConsumerRef = consumer;
                consumer.subscribe(List.of(topic));
                log.info("[SINGLE] subscribed to {}", topic);

                while (singleRunning) {
                    try {
                        ConsumerRecords<String, Message> records = consumer.poll(Duration.ofMillis(300));
                        for (var r : records) {
                            try {
                                var m = r.value();
                                log.info("[SINGLE] key={} value={} p={} off={}", r.key(), m, r.partition(), r.offset());
                            } catch (Exception e) {
                                log.error("[SINGLE] processing error", e);
                            }
                        }
                    } catch (org.apache.kafka.common.errors.SerializationException se) {
                        log.error("[SINGLE] deserialization problem: {}", se.getMessage(), se);
                    }
                }
            } catch (WakeupException we) {
                if (singleRunning) log.warn("[SINGLE] wakeup", we);
            } catch (Exception e) {
                log.error("[SINGLE] fatal", e);
            } finally {
                singleRunning = false;
                singleConsumerRef = null;
                log.info("[SINGLE] stopped");
            }
        }, "single-consumer");
        singleThread.setDaemon(true);
        singleThread.start();
    }

    public void batchConsumeMessage(String topic) {
        if (batchRunning) {
            log.info("[BATCH] already running on topic={}", topic);
            return;
        }
        batchRunning = true;

        batchThread = new Thread(() -> {
            try (KafkaConsumer<String, Message> consumer = new KafkaConsumer<>(kafkaConfig.batchConsumerProps())) {
                batchConsumerRef = consumer;
                consumer.subscribe(List.of(topic));
                log.info("[BATCH] subscribed to {}", topic);

                final int minBatch = 10;
                final long assembleTimeoutMs = 1000;
                var buffer = new ArrayList<ConsumerRecord<String, Message>>(minBatch);
                long started = System.currentTimeMillis();

                while (batchRunning) {
                    try {
                        ConsumerRecords<String, Message> polled = consumer.poll(Duration.ofMillis(300));
                        polled.forEach(buffer::add);

                        boolean enough = buffer.size() >= minBatch;
                        boolean timeout = System.currentTimeMillis() - started >= assembleTimeoutMs;

                        if (enough || (timeout && !buffer.isEmpty())) {
                            try {
                                for (var r : buffer) {
                                    var m = r.value();
                                    log.info("[BATCH] key={} value={} p={} off={}", r.key(), m, r.partition(), r.offset());
                                }
                                consumer.commitSync();
                                log.info("[BATCH] committed {} records", buffer.size());
                            } catch (Exception e) {
                                log.error("[BATCH] processing error; will re-read (no commit)", e);
                            } finally {
                                buffer.clear();
                                started = System.currentTimeMillis();
                            }
                        } else if (timeout) {
                            started = System.currentTimeMillis();
                        }
                    } catch (SerializationException se) {
                        log.error("[BATCH] deserialization problem: {}", se.getMessage(), se);
                    }
                }
            } catch (WakeupException we) {
                if (batchRunning) log.warn("[BATCH] wakeup", we);
            } catch (Exception e) {
                log.error("[BATCH] fatal", e);
            } finally {
                batchRunning = false;
                batchConsumerRef = null;
                log.info("[BATCH] stopped");
            }
        }, "batch-consumer");
        batchThread.setDaemon(true);
        batchThread.start();
    }

    public void stopAll() {
        singleRunning = false;
        batchRunning = false;
        if (singleConsumerRef != null) singleConsumerRef.wakeup();
        if (batchConsumerRef != null) batchConsumerRef.wakeup();
    }
}
