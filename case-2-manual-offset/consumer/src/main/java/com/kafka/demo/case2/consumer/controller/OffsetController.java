package com.kafka.demo.case2.consumer.controller;

import com.kafka.demo.case2.consumer.entity.KafkaOffsetEntity;
import com.kafka.demo.case2.consumer.entity.ProcessedOrderEntity;
import com.kafka.demo.case2.consumer.repository.KafkaOffsetRepository;
import com.kafka.demo.case2.consumer.repository.ProcessedOrderRepository;
import com.kafka.demo.case2.consumer.service.ManualOffsetService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API для управления оффсетами и просмотра обработанных заказов.
 */
@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class OffsetController {

    private static final String TOPIC = "manual-offset-topic";

    private final KafkaOffsetRepository offsetRepository;
    private final ProcessedOrderRepository processedOrderRepository;
    private final ManualOffsetService manualOffsetService;

    @GetMapping("/offsets")
    public ResponseEntity<List<KafkaOffsetEntity>> getAllOffsets() {
        return ResponseEntity.ok(offsetRepository.findAll());
    }

    @GetMapping("/processed-orders")
    public ResponseEntity<List<ProcessedOrderEntity>> getProcessedOrders() {
        return ResponseEntity.ok(processedOrderRepository.findAll());
    }

    /**
     * Ручной сдвиг оффсета для replay.
     *
     * POST /api/offsets/seek  { "partition": 0, "offset": 5 }
     * После вызова перезапустить консьюмер — он начнёт читать с offset+1.
     */
    @PostMapping("/offsets/seek")
    public ResponseEntity<Map<String, Object>> seekOffset(@RequestBody Map<String, Object> body) {
        int partition = ((Number) body.get("partition")).intValue();
        long offset = ((Number) body.get("offset")).longValue();

        manualOffsetService.upsertOffset(TOPIC, partition, offset, ManualOffsetService.GROUP);

        return ResponseEntity.ok(Map.of(
            "status", "offset updated",
            "partition", partition,
            "newOffset", offset,
            "note", "Restart the consumer service to apply the new offset"
        ));
    }
}
