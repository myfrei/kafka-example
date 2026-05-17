package com.kafka.demo.case1.producer.controller;

import com.kafka.demo.case1.producer.model.OrderEvent;
import com.kafka.demo.case1.producer.service.OrderFactory;
import com.kafka.demo.case1.producer.service.OrderProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API продюсера Case 1.
 *
 * Позволяет либо отправить готовый {@link OrderEvent}, либо попросить
 * сгенерировать партию случайных заказов.
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducerService producerService;
    private final OrderFactory orderFactory;

    @Value("${app.producer-id}")
    private String producerId;

    /**
     * POST /api/orders — отправить заказ с заданным телом.
     * Тело — JSON объекта {@link OrderEvent}.
     */
    @PostMapping
    public ResponseEntity<OrderEvent> sendOrder(@RequestBody OrderEvent order) {
        return ResponseEntity.ok(producerService.publish(order));
    }

    /**
     * POST /api/orders/random?count=10 — сгенерировать и отправить N случайных заказов.
     */
    @PostMapping("/random")
    public ResponseEntity<Map<String, Object>> sendRandom(@RequestParam(defaultValue = "10") int count) {
        List<String> ids = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            OrderEvent order = orderFactory.newOrder(producerId);
            producerService.publish(order);
            ids.add(order.orderId());
        }
        return ResponseEntity.ok(Map.of("sent", count, "orderIds", ids));
    }

    @GetMapping("/health")
    public ResponseEntity<String> health() {
        return ResponseEntity.ok("Producer " + producerId + " is running");
    }
}
