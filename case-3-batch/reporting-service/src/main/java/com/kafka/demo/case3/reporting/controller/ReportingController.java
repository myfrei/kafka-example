package com.kafka.demo.case3.reporting.controller;

import com.kafka.demo.case3.reporting.service.ReportingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** GET /api/report — накопительный отчёт по батчам. */
@RestController
@RequestMapping("/api/report")
@RequiredArgsConstructor
public class ReportingController {

    private final ReportingService reportingService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> report() {
        return ResponseEntity.ok(Map.of(
            "batches", reportingService.batchCount(),
            "records", reportingService.recordCount(),
            "totalRevenue", reportingService.totalRevenue()
        ));
    }
}
