package com.kafka.demo.case6.producer.controller;

import com.kafka.demo.case6.producer.service.TransactionalProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API транзакционного продюсера.
 *
 * POST /api/orders        — заказ отправляется атомарно (commit)
 * POST /api/orders/fail   — транзакция откатывается (rollback)
 */
@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final TransactionalProducerService producerService;

    public record OrderRequest(String orderId, List<String> items) {}

    @PostMapping
    public ResponseEntity<Map<String, Object>> send(@RequestBody OrderRequest request) {
        List<String> items = request.items() == null ? List.of() : request.items();
        String correlationId = producerService.sendOrderWithItems(request.orderId(), items);
        return ResponseEntity.ok(Map.of(
            "orderId", request.orderId(),
            "correlationId", correlationId,
            "messages", items.size() + 2,
            "status", "committed"
        ));
    }

    @PostMapping("/fail")
    public ResponseEntity<Map<String, Object>> sendAndFail(@RequestBody OrderRequest request) {
        try {
            producerService.sendAndFail(request.orderId());
            return ResponseEntity.ok(Map.of("status", "unexpected — should have failed"));
        } catch (RuntimeException e) {
            return ResponseEntity.ok(Map.of(
                "orderId", request.orderId(),
                "status", "rolled back",
                "reason", e.getMessage(),
                "note", "consumers with read_committed will NOT see these messages"
            ));
        }
    }
}
