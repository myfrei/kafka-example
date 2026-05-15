package com.kafka.demo.case1.producer.model;

import java.math.BigDecimal;

/**
 * Позиция заказа — один товар в составе заказа.
 *
 * @param sku       артикул товара
 * @param name      человекочитаемое название
 * @param quantity  количество единиц
 * @param unitPrice цена за единицу
 */
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
