package com.kafka.demo.case1.analytics.model;

import java.math.BigDecimal;

public record OrderLine(
    String sku,
    String name,
    int quantity,
    BigDecimal unitPrice
) {
}
