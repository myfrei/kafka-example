package com.kafka.demo.case1.analytics.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderEvent(
    String orderId,
    String customerId,
    String region,
    List<OrderLine> lines,
    BigDecimal totalAmount,
    String producerId,
    Instant createdAt
) {
}
