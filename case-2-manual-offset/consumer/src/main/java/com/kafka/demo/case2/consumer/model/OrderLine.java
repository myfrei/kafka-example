package com.kafka.demo.case2.consumer.model;

import java.math.BigDecimal;

/** Позиция заказа. */
public record OrderLine(
    String sku,
    String name,
    int quantity,
    BigDecimal unitPrice
) {
}
