package com.kafka.demo.case7.consumer.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Событие платежа (копия record-а продюсера — модули независимы, общего модуля нет).
 * customerId "bad-*" → «ядовитый» платёж. [ГОТОВО — не требует изменений]
 */
public record PaymentEvent(
    String paymentId,
    String orderId,
    String customerId,
    BigDecimal amount,
    Instant createdAt
) {
}
