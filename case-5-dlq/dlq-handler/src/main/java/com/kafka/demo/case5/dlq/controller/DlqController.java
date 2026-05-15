package com.kafka.demo.case5.dlq.controller;

import com.kafka.demo.case5.dlq.model.DlqRecord;
import com.kafka.demo.case5.dlq.service.DlqService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** GET /api/dlq — список сообщений, попавших в DLQ. */
@RestController
@RequestMapping("/api/dlq")
@RequiredArgsConstructor
public class DlqController {

    private final DlqService dlqService;

    @GetMapping
    public ResponseEntity<Map<String, Object>> dlq() {
        List<DlqRecord> records = dlqService.all();
        return ResponseEntity.ok(Map.of("count", records.size(), "records", records));
    }
}
