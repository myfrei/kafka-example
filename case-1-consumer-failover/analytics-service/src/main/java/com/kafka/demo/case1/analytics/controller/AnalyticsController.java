package com.kafka.demo.case1.analytics.controller;

import com.kafka.demo.case1.analytics.service.RevenueAnalyticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * REST API аналитики: выручка и число заказов по регионам.
 *
 * GET /api/analytics
 */
@RestController
@RequestMapping("/api/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final RevenueAnalyticsService analyticsService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> analytics() {
        return ResponseEntity.ok(Map.of(
            "revenueByRegion", analyticsService.revenueByRegion(),
            "ordersByRegion", analyticsService.ordersByRegion()
        ));
    }
}
