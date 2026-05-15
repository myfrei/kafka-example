package com.kafka.demo.case3.consumer.model;

import java.math.BigDecimal;

public record OrderLine(String sku, String name, int quantity, BigDecimal unitPrice) {
}
