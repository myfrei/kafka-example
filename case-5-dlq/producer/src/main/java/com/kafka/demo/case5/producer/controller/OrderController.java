package com.kafka.demo.case5.producer.controller;

import com.kafka.demo.case5.producer.model.OrderEvent;
import com.kafka.demo.case5.producer.service.OrderProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API продюсера Case 5.
 *
 * /random  — нормальные заказы (обрабатываются успешно)
 * /poison  — «ядовитые» заказы (всегда падают → DLQ)
 * POST /   — отправить произвольный заказ
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducerService producerService;

    @PostMapping
    public ResponseEntity<OrderEvent> send(@RequestBody OrderEvent order) {
        return ResponseEntity.ok(producerService.publish(order));
    }

    @PostMapping("/random")
    public ResponseEntity<Map<String, Object>> random(@RequestParam(defaultValue = "10") int count) {
        for (int i = 0; i < count; i++) {
            producerService.publishGood();
        }
        return ResponseEntity.ok(Map.of("sent", count, "type", "good"));
    }

    @PostMapping("/poison")
    public ResponseEntity<Map<String, Object>> poison(@RequestParam(defaultValue = "3") int count) {
        for (int i = 0; i < count; i++) {
            producerService.publishPoison();
        }
        return ResponseEntity.ok(Map.of("sent", count, "type", "poison", "note", "these will land in orders-dlq-demo.DLT"));
    }
}
