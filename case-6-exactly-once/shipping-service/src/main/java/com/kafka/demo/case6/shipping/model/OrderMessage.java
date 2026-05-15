package com.kafka.demo.case6.shipping.model;

import java.time.Instant;

public record OrderMessage(
    String type,
    String orderId,
    String correlationId,
    String item,
    int sequence,
    Instant timestamp
) {
}
