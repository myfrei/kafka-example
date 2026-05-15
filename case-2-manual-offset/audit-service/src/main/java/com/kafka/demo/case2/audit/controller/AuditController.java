package com.kafka.demo.case2.audit.controller;

import com.kafka.demo.case2.audit.service.AuditService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/** GET /api/audit — журнал аудита заказов. */
@RestController
@RequestMapping("/api/audit")
@RequiredArgsConstructor
public class AuditController {

    private final AuditService auditService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> audit() {
        return ResponseEntity.ok(Map.of(
            "totalAudited", auditService.totalAudited(),
            "recent", auditService.recent()
        ));
    }
}
