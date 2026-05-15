package com.kafka.demo.case5.dlq.model;

import java.math.BigDecimal;
import java.time.Instant;

/** Заказ, прочитанный из DLQ-топика. */
public record OrderEvent(
    String orderId,
    String customerId,
    BigDecimal amount,
    String note,
    Instant createdAt
) {
}
