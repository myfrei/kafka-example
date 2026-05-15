package com.kafka.demo.case4.service.controller;

import com.kafka.demo.case4.service.service.ActivityProcessingService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * GET /api/partition-stats — какие партиции читает этот инстанс и сколько событий обработал.
 */
@RestController
@RequestMapping("/api/partition-stats")
@RequiredArgsConstructor
public class PartitionStatsController {

    private final ActivityProcessingService processingService;

    @Value("${app.instance-id}")
    private String instanceId;

    @GetMapping
    public ResponseEntity<Map<String, Object>> stats() {
        return ResponseEntity.ok(Map.of(
            "instanceId", instanceId,
            "total", processingService.total(),
            "countByPartition", processingService.countByPartition()
        ));
    }
}
