package com.kafka.demo.case2.producer.model;

import java.math.BigDecimal;

/** Позиция заказа. */
public record OrderLine(
    String sku,
    String name,
    int quantity,
    BigDecimal unitPrice
) {
    public BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
