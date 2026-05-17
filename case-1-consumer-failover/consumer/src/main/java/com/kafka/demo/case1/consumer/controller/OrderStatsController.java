package com.kafka.demo.case1.consumer.controller;

import com.kafka.demo.case1.consumer.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Показывает, сколько заказов обработал этот инстанс консьюмера.
 *
 * Удобно при демонстрации failover: после остановки одного инстанса видно,
 * как у оставшегося растёт счётчик обработанных заказов.
 */
@RestController
@RequestMapping("/api/stats")
@RequiredArgsConstructor
public class OrderStatsController {

    private final OrderProcessingService processingService;

    @Value("${app.consumer-instance-id}")
    private String instanceId;

    @GetMapping
    public ResponseEntity<Map<String, Object>> stats() {
        return ResponseEntity.ok(Map.of(
            "instanceId", instanceId,
            "processedCount", processingService.processedCount(),
            "distinctOrders", processingService.distinctOrderCount()
        ));
    }
}
