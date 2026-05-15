package com.kafka.demo.case3.consumer.controller;

import com.kafka.demo.case3.consumer.service.BatchProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** GET /api/batch-stats — сколько записей и батчей обработано. */
@RestController
@RequestMapping("/api/batch-stats")
@RequiredArgsConstructor
public class BatchStatsController {

    private final BatchProcessingService processingService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> stats() {
        long records = processingService.totalRecordsProcessed();
        long batches = processingService.totalBatches();
        return ResponseEntity.ok(Map.of(
            "totalRecordsProcessed", records,
            "totalBatches", batches,
            "avgBatchSize", batches == 0 ? 0 : records / batches
        ));
    }
}
