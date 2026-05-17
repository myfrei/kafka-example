package com.kafka.demo.case2.producer.controller;

import com.kafka.demo.case2.producer.model.OrderEvent;
import com.kafka.demo.case2.producer.service.OrderFactory;
import com.kafka.demo.case2.producer.service.OrderProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * REST API продюсера Case 2.
 *
 * Случайные заказы можно слать пачками — потом удобно «проигрывать» (replay)
 * их повторно, сдвигая оффсет в PostgreSQL.
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderProducerService producerService;
    private final OrderFactory orderFactory;

    @PostMapping
    public ResponseEntity<OrderEvent> sendOrder(@RequestBody OrderEvent order) {
        return ResponseEntity.ok(producerService.publish(order));
    }

    @PostMapping("/random")
    public ResponseEntity<Map<String, Object>> sendRandom(@RequestParam(defaultValue = "10") int count) {
        List<String> ids = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            ids.add(producerService.publish(orderFactory.newOrder()).orderId());
        }
        return ResponseEntity.ok(Map.of("sent", count, "orderIds", ids));
    }
}
