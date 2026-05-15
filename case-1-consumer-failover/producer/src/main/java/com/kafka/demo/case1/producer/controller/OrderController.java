package com.kafka.demo.case1.producer.controller;

import com.kafka.demo.case1.producer.service.OrderProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducerService producerService;

    /**
     * POST /api/orders
     * Тело: { "key": "order-123", "message": "..." }
     *
     * Позволяет вручную отправить сообщение с конкретным ключом.
     * Ключ важен: если отправлять сообщения с одним ключом,
     * они всегда попадут в одну партицию (гарантия порядка).
     */
    @PostMapping
    public ResponseEntity<Map<String, String>> sendOrder(@RequestBody Map<String, String> body) {
        String key = body.getOrDefault("key", "manual-key");
        String message = body.getOrDefault("message", "{}");
        producerService.sendMessage(key, message);
        return ResponseEntity.ok(Map.of("status", "sent", "key", key));
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Producer is running");
    }
}
