package com.kafka.demo.case2.consumer.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Событие заказа, прочитанное из топика manual-offset-topic. */
public record OrderEvent(
    String orderId,
    String customerId,
    String region,
    List<OrderLine> lines,
    BigDecimal totalAmount,
    Instant createdAt
) {
}
