package com.kafka.demo.case4.producer.controller;

import com.kafka.demo.case4.producer.model.ActivityEvent;
import com.kafka.demo.case4.producer.service.ActivityFactory;
import com.kafka.demo.case4.producer.service.ActivityProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * Продюсер для Case 4.
 *
 * Демонстрирует, как ключ сообщения (customerId) влияет на выбор партиции:
 * partition = murmur2(key) % numPartitions.
 */
@Slf4j
@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class PartitionedProducerController {

    private final ActivityProducerService producerService;
    private final ActivityFactory activityFactory;

    /** POST /api/messages?customerId=customer-7 — отправить событие с ключом. */
    @PostMapping
    public ResponseEntity<Map<String, Object>> send(@RequestParam String customerId) {
        ActivityEvent event = activityFactory.newActivity(customerId);
        producerService.publish(event);
        return ResponseEntity.ok(Map.of("customerId", customerId, "status", "sent"));
    }

    /** GET /api/messages/flood?count=30 — N событий случайных покупателей. */
    @GetMapping("/flood")
    public ResponseEntity<Map<String, Object>> flood(@RequestParam(defaultValue = "30") int count) {
        for (int i = 0; i < count; i++) {
            producerService.publish(activityFactory.newActivityForRandomCustomer());
        }
        return ResponseEntity.ok(Map.of(
            "sent", count,
            "note", "Check Kafka UI → partitioned-topic → Partitions for distribution"
        ));
    }

    /** POST /api/messages/partition/2?customerId=any — отправить в конкретную партицию. */
    @PostMapping("/partition/{partition}")
    public ResponseEntity<Map<String, Object>> sendToPartition(
        @PathVariable int partition,
        @RequestParam String customerId
    ) {
        producerService.publishToPartition(partition, activityFactory.newActivity(customerId));
        return ResponseEntity.ok(Map.of(
            "customerId", customerId,
            "forcedPartition", partition,
            "status", "sent"
        ));
    }
}
