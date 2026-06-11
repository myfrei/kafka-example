package com.kafka.demo.case7.consumer.controller;

import com.kafka.demo.case7.consumer.service.PaymentProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * GET /api/retry-stats — счётчики обработки. [ГОТОВО — не требует изменений]
 * Удобно дёргать во время проверки: сколько платежей обработано и сколько ушло в DLT.
 */
@RestController
@RequestMapping("/api/retry-stats")
@RequiredArgsConstructor
public class RetryStatsController {

    private final PaymentProcessingService processingService;

    @GetMapping
    public Map<String, Object> stats() {
        return Map.of(
            "processed", processingService.processedCount(),
            "dlt", processingService.dltCount()
        );
    }
}
