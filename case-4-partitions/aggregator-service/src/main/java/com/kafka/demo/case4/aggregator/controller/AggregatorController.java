package com.kafka.demo.case4.aggregator.controller;

import com.kafka.demo.case4.aggregator.service.AggregatorService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** GET /api/aggregate — активность по типам и регионам (по всем партициям). */
@RestController
@RequestMapping("/api/aggregate")
@RequiredArgsConstructor
public class AggregatorController {

    private final AggregatorService aggregatorService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> aggregate() {
        return ResponseEntity.ok(Map.of(
            "byActivityType", aggregatorService.byType(),
            "byRegion", aggregatorService.byRegion()
        ));
    }
}
