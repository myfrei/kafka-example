package com.kafka.demo.case2.consumer.controller;

import com.kafka.demo.case2.consumer.entity.KafkaOffsetEntity;
import com.kafka.demo.case2.consumer.repository.KafkaOffsetRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST API для управления оффсетами.
 *
 * Позволяет:
 * 1. Посмотреть текущие сохранённые оффсеты
 * 2. Вручную сдвинуть оффсет (для replay)
 *
 * После изменения оффсета через API — перезапустить сервис,
 * и он начнёт читать с нового места.
 */
@RestController
@RequestMapping("/api/offsets")
@RequiredArgsConstructor
public class OffsetController {

    private final KafkaOffsetRepository offsetRepository;

    @GetMapping
    public ResponseEntity<List<KafkaOffsetEntity>> getAllOffsets() {
        return ResponseEntity.ok(offsetRepository.findAll());
    }

    /**
     * Ручной сдвиг оффсета для replay.
     *
     * Пример: сдвинуть оффсет партиции 0 на значение 5
     * → после перезапуска сервис начнёт читать с offset=6
     *
     * POST /api/offsets/seek
     * { "partition": 0, "offset": 5 }
     */
    @PostMapping("/seek")
    @Transactional
    public ResponseEntity<Map<String, Object>> seekOffset(@RequestBody Map<String, Object> body) {
        int partition = (Integer) body.get("partition");
        long offset = ((Number) body.get("offset")).longValue();

        offsetRepository.upsertOffset("manual-offset-topic", partition, offset, "manual-offset-group");

        return ResponseEntity.ok(Map.of(
            "status", "offset updated",
            "partition", partition,
            "newOffset", offset,
            "note", "Restart the consumer service to apply new offset"
        ));
    }
}
