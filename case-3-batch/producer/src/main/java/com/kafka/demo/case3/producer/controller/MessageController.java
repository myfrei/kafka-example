package com.kafka.demo.case3.producer.controller;

import com.kafka.demo.case3.producer.model.OrderEvent;
import com.kafka.demo.case3.producer.service.OrderFactory;
import com.kafka.demo.case3.producer.service.OrderProducerService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API продюсера Case 3.
 *
 * Главный эндпоинт — flood: заливает много заказов сразу, чтобы батч-консьюмер
 * успевал набирать полные батчи (до 50 записей за poll()).
 */
@Slf4j
@RestController
@RequestMapping("/api/messages")
@RequiredArgsConstructor
public class MessageController {

    private final OrderProducerService producerService;
    private final OrderFactory orderFactory;

    @PostMapping
    public ResponseEntity<OrderEvent> send(@RequestBody OrderEvent order) {
        return ResponseEntity.ok(producerService.publish(order));
    }

    /**
     * GET /api/messages/flood?count=200 — залить N случайных заказов.
     */
    @GetMapping("/flood")
    public ResponseEntity<Map<String, Object>> flood(@RequestParam(defaultValue = "200") int count) {
        for (int i = 0; i < count; i++) {
            producerService.publish(orderFactory.newOrder());
        }
        log.info("Flooded batch-topic with {} orders", count);
        return ResponseEntity.ok(Map.of("sent", count));
    }
}
