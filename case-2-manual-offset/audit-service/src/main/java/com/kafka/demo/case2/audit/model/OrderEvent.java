package com.kafka.demo.case2.audit.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderEvent(
    String orderId,
    String customerId,
    String region,
    List<OrderLine> lines,
    BigDecimal totalAmount,
    Instant createdAt
) {
}
