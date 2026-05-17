package com.kafka.demo.case2.audit.model;

import java.math.BigDecimal;

public record OrderLine(String sku, String name, int quantity, BigDecimal unitPrice) {
}
