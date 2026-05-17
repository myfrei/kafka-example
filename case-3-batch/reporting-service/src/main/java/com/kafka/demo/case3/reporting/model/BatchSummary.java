package com.kafka.demo.case3.reporting.model;

import java.math.BigDecimal;
import java.time.Instant;

/** Сводка по батчу, прочитанная из топика batch-summary. */
public record BatchSummary(
    String batchId,
    int recordCount,
    int distinctCustomers,
    BigDecimal totalRevenue,
    Instant processedAt
) {
}
