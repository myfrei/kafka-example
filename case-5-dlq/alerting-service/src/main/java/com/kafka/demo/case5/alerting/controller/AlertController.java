package com.kafka.demo.case5.alerting.controller;

import com.kafka.demo.case5.alerting.service.AlertingService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** GET /api/alerts — статистика алертов по типам ошибок. */
@RestController
@RequestMapping("/api/alerts")
@RequiredArgsConstructor
public class AlertController {

    private final AlertingService alertingService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> alerts() {
        return ResponseEntity.ok(Map.of(
            "totalAlerts", alertingService.totalAlerts(),
            "byExceptionType", alertingService.alertsByException()
        ));
    }
}
