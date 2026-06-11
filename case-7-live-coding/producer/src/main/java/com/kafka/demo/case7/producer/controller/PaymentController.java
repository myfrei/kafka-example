package com.kafka.demo.case7.producer.controller;

import com.kafka.demo.case7.producer.model.PaymentEvent;
import com.kafka.demo.case7.producer.service.PaymentProducerService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * REST API продюсера Case 7. [ГОТОВО — не требует изменений]
 *
 * POST /api/payments               — отправить произвольный платёж (JSON-тело)
 * POST /api/payments/random?count  — N обычных платежей
 * POST /api/payments/poison?count  — N «ядовитых» платежей (уйдут в DLT)
 */
@RestController
@RequestMapping("/api/payments")
@RequiredArgsConstructor
public class PaymentController {

    private final PaymentProducerService producerService;

    @PostMapping
    public ResponseEntity<PaymentEvent> send(@RequestBody PaymentEvent payment) {
        return ResponseEntity.ok(producerService.publish(payment));
    }

    @PostMapping("/random")
    public ResponseEntity<Map<String, Object>> random(@RequestParam(defaultValue = "20") int count) {
        for (int i = 0; i < count; i++) {
            producerService.publishRandom();
        }
        return ResponseEntity.ok(Map.of("sent", count, "type", "random"));
    }

    @PostMapping("/poison")
    public ResponseEntity<Map<String, Object>> poison(@RequestParam(defaultValue = "3") int count) {
        for (int i = 0; i < count; i++) {
            producerService.publishPoison();
        }
        return ResponseEntity.ok(Map.of("sent", count, "type", "poison",
            "note", "should land in payments-retry-demo-dlt without retries"));
    }
}
