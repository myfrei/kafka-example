package com.kafka.demo.case1.consumer.model;

import java.math.BigDecimal;

/**
 * Позиция заказа — один товар в составе заказа.
 */
public record OrderLine(
    String sku,
    String name,
    int quantity,
    BigDecimal unitPrice
) {
}
