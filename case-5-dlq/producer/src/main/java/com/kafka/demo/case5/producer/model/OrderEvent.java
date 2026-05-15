package com.kafka.demo.case5.producer.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Событие заказа для демонстрации DLQ.
 *
 * Заказы покупателей с customerId, начинающимся на "bad-", считаются «ядовитыми»
 * (poison messages) — их обработка всегда падает, и они уходят в DLQ.
 */
public record OrderEvent(
    String orderId,
    String customerId,
    BigDecimal amount,
    String note,
    Instant createdAt
) {
}
