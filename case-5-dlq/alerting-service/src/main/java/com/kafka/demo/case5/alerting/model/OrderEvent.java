package com.kafka.demo.case5.alerting.model;

import java.math.BigDecimal;
import java.time.Instant;

public record OrderEvent(
    String orderId,
    String customerId,
    BigDecimal amount,
    String note,
    Instant createdAt
) {
}
