package com.kafka.demo.case3.consumer.service;

import com.kafka.demo.case3.consumer.model.BatchSummary;
import com.kafka.demo.case3.consumer.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Бизнес-логика батч-обработки.
 *
 * Демонстрирует типичную батч-операцию: вместо N отдельных INSERT'ов —
 * один bulk-insert. Здесь это имитируется, а результат сворачивается в {@link BatchSummary}.
 */
@Slf4j
@Service
public class BatchProcessingService {

    private final AtomicLong totalRecords = new AtomicLong();
    private final AtomicLong totalBatches = new AtomicLong();

    /**
     * Обрабатывает батч заказов и возвращает сводку.
     */
    public BatchSummary processBatch(List<OrderEvent> orders) {
        long distinctCustomers = orders.stream()
            .map(OrderEvent::customerId)
            .distinct()
            .count();

        BigDecimal revenue = orders.stream()
            .map(OrderEvent::totalAmount)
            .reduce(BigDecimal.ZERO, BigDecimal::add);

        // Имитация bulk-insert: одна операция на весь батч
        simulateBulkInsert(orders.size());

        totalRecords.addAndGet(orders.size());
        totalBatches.incrementAndGet();

        BatchSummary summary = new BatchSummary(
            "batch-" + UUID.randomUUID().toString().substring(0, 8),
            orders.size(),
            (int) distinctCustomers,
            revenue,
            Instant.now()
        );
        log.info("Batch processed: {} orders, {} distinct customers, revenue={}",
            summary.recordCount(), summary.distinctCustomers(), summary.totalRevenue());
        return summary;
    }

    public long totalRecordsProcessed() {
        return totalRecords.get();
    }

    public long totalBatches() {
        return totalBatches.get();
    }

    private void simulateBulkInsert(int size) {
        try {
            Thread.sleep(Math.min(size * 2L, 200));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Утилита: дедупликация заказов по orderId внутри батча. */
    public List<OrderEvent> deduplicate(List<OrderEvent> orders) {
        return orders.stream()
            .collect(Collectors.toMap(OrderEvent::orderId, o -> o, (a, b) -> b))
            .values()
            .stream()
            .toList();
    }
}
