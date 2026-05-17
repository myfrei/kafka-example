package com.kafka.demo.case1.consumer.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Событие заказа — типизированная модель, в которую JsonDeserializer превращает
 * сообщение из топика "orders".
 */
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
