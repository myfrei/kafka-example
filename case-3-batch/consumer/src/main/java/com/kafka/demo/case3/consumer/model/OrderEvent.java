package com.kafka.demo.case3.consumer.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Событие заказа, прочитанное батчами из топика batch-topic. */
public record OrderEvent(
    String orderId,
    String customerId,
    String region,
    List<OrderLine> lines,
    BigDecimal totalAmount,
    Instant createdAt
) {
}
