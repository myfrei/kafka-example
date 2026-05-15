package com.kafka.demo.case5.consumer.controller;

import com.kafka.demo.case5.consumer.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** GET /api/processing-stats — сколько заказов обработано успешно. */
@RestController
@RequestMapping("/api/processing-stats")
@RequiredArgsConstructor
public class ProcessingStatsController {

    private final OrderProcessingService processingService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> stats() {
        return ResponseEntity.ok(Map.of("processedSuccessfully", processingService.processedCount()));
    }
}
