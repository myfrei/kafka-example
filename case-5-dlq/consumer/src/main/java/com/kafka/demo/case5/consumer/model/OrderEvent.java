package com.kafka.demo.case5.consumer.model;

import java.math.BigDecimal;
import java.time.Instant;

/** Заказ, прочитанный из топика orders-dlq-demo. */
public record OrderEvent(
    String orderId,
    String customerId,
    BigDecimal amount,
    String note,
    Instant createdAt
) {
}
