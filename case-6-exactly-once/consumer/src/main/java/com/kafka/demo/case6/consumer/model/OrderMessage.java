package com.kafka.demo.case6.consumer.model;

import java.time.Instant;

/** Сообщение заказа, прочитанное из exactly-once-topic. */
public record OrderMessage(
    String type,
    String orderId,
    String correlationId,
    String item,
    int sequence,
    Instant timestamp
) {
}
