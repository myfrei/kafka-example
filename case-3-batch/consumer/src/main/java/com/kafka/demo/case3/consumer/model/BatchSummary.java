package com.kafka.demo.case3.consumer.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Сводка по обработанному батчу.
 *
 * Батч-консьюмер публикует её в топик batch-summary после успешного коммита батча.
 * reporting-service агрегирует эти сводки в отчёт.
 */
public record BatchSummary(
    String batchId,
    int recordCount,
    int distinctCustomers,
    BigDecimal totalRevenue,
    Instant processedAt
) {
}
