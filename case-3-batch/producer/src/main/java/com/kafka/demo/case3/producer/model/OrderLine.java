package com.kafka.demo.case3.producer.model;

import java.math.BigDecimal;

public record OrderLine(String sku, String name, int quantity, BigDecimal unitPrice) {
    public BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
