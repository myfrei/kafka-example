package com.kafka.demo.case7.producer.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Событие платежа для демонстрации НЕблокирующих повторов (retry-топики).
 *
 * Платежи покупателей с customerId, начинающимся на "bad-", считаются «ядовитыми»
 * (poison): их обработка падает детерминированно — такие сообщения должны уходить
 * в DLT без повторов. Остальные сбои — временные (transient) и повторяемые.
 *
 * customerId используется как КЛЮЧ сообщения → определяет партицию (порядок платежей
 * одного покупателя). [ГОТОВО — не требует изменений]
 */
public record PaymentEvent(
    String paymentId,
    String orderId,
    String customerId,
    BigDecimal amount,
    Instant createdAt
) {
}
