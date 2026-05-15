package com.kafka.demo.case4.producer.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Продюсер для Case 4.
 *
 * Демонстрирует как ключ сообщения влияет на выбор партиции.
 * Ключи с одинаковым hash всегда попадают в одну партицию.
 *
 * Алгоритм: partition = murmur2(key.getBytes()) % numPartitions
 */
@Slf4j
@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class PartitionedProducerController {

    private static final String TOPIC = "partitioned-topic";
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Отправить одно сообщение с ключом.
     * Kafka детерминированно выберет партицию на основе ключа.
     *
     * POST /api/messages
     * { "key": "user-123", "value": "some data" }
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> send(@RequestBody Map<String, String> body) {
        String key = body.get("key");
        String value = body.getOrDefault("value", "{}");

        var future = kafkaTemplate.send(TOPIC, key, value);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent key={} → partition={} offset={}",
                    key,
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset()
                );
            }
        });

        return ResponseEntity.ok(Map.of("key", key, "status", "sent"));
    }

    /**
     * Отправить N сообщений с разными ключами для наблюдения распределения по партициям.
     *
     * GET /api/messages/flood?count=30
     */
    @GetMapping("/flood")
    public ResponseEntity<Map<String, Object>> flood(
        @RequestParam(defaultValue = "30") int count
    ) {
        List<String> keys = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            String key = "user-" + i;
            String value = String.format("{\"userId\": %d, \"action\": \"login\"}", i);
            kafkaTemplate.send(TOPIC, key, value);
            keys.add(key);
        }
        log.info("Flooded topic with {} messages", count);
        return ResponseEntity.ok(Map.of(
            "sent", count,
            "note", "Check Kafka UI → partitioned-topic → Partitions to see distribution"
        ));
    }

    /**
     * Отправить сообщения с явным указанием партиции (bypass key-routing).
     *
     * POST /api/messages/partition/1
     * { "key": "any", "value": "forced to partition 1" }
     */
    @PostMapping("/partition/{partition}")
    public ResponseEntity<Map<String, Object>> sendToPartition(
        @PathVariable int partition,
        @RequestBody Map<String, String> body
    ) {
        String key = body.get("key");
        String value = body.getOrDefault("value", "{}");
        kafkaTemplate.send(TOPIC, partition, key, value);
        return ResponseEntity.ok(Map.of(
            "key", key,
            "forcedPartition", partition,
            "status", "sent"
        ));
    }
}
