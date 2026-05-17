package com.kafka.demo.case6.consumer.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** GET /api/processed — обработанные сообщения и заказы из PostgreSQL. */
@RestController
@RequestMapping("/api/processed")
@RequiredArgsConstructor
public class ProcessedController {

    private final JdbcTemplate jdbcTemplate;

    @GetMapping
    public ResponseEntity<Map<String, Object>> processed() {
        Integer messages = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM processed_messages", Integer.class);
        Integer orders = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM orders", Integer.class);
        List<Map<String, Object>> recent = jdbcTemplate.queryForList(
            "SELECT message_id, payload, processed_at FROM processed_messages "
                + "ORDER BY id DESC LIMIT 20");
        return ResponseEntity.ok(Map.of(
            "processedMessages", messages,
            "orders", orders,
            "recent", recent
        ));
    }
}
