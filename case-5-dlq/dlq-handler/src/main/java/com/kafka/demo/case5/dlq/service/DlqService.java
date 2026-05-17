package com.kafka.demo.case5.dlq.service;

import com.kafka.demo.case5.dlq.model.DlqRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Хранилище DLQ-сообщений (in-memory; в реальной системе — PostgreSQL).
 *
 * Даёт операторам возможность просмотреть проблемные сообщения и проанализировать
 * причины. В реальной системе здесь же был бы re-submit (повторная отправка
 * в исходный топик после исправления).
 */
@Slf4j
@Service
public class DlqService {

    private final List<DlqRecord> records = new CopyOnWriteArrayList<>();

    public void store(DlqRecord record) {
        records.add(record);
        log.error("""
            ╔═══════════════════════════════════════════╗
            ║  DLQ MESSAGE — REQUIRES ATTENTION
            ╠═══════════════════════════════════════════╣
            ║  Order:      {}
            ║  Customer:   {}
            ║  Origin:     {}-{} @ offset {}
            ║  Error:      {}: {}
            ╚═══════════════════════════════════════════╝""",
            record.orderId(), record.customerId(),
            record.originalTopic(), record.originalPartition(), record.originalOffset(),
            record.exceptionType(), record.exceptionMessage());
    }

    public List<DlqRecord> all() {
        return new ArrayList<>(records);
    }

    public int count() {
        return records.size();
    }
}
