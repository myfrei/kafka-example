package com.kafka.demo.case2.producer.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/** Событие заказа, публикуемое в топик manual-offset-topic. */
public record OrderEvent(
    String orderId,
    String customerId,
    String region,
    List<OrderLine> lines,
    BigDecimal totalAmount,
    Instant createdAt
) {
}
