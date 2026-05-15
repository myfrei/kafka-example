package com.kafka.demo.case1.producer.service;

import com.kafka.demo.case1.producer.model.OrderEvent;
import com.kafka.demo.case1.producer.model.OrderLine;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Фабрика реалистичных заказов.
 *
 * Генерирует случайные, но правдоподобные данные: разные регионы, наборы товаров,
 * количество позиций. Это делает демо живее — в логах консьюмеров видно осмысленный
 * поток заказов, а не одинаковые заглушки.
 */
@Component
public class OrderFactory {

    private static final String[] REGIONS = {"EU-WEST", "EU-EAST", "US-EAST", "APAC"};

    private record Product(String sku, String name, BigDecimal price) {}

    private static final Product[] CATALOG = {
        new Product("SKU-LAPTOP", "Laptop Pro 14", new BigDecimal("1499.00")),
        new Product("SKU-PHONE", "Smartphone X", new BigDecimal("899.00")),
        new Product("SKU-HEADSET", "Wireless Headset", new BigDecimal("129.50")),
        new Product("SKU-MOUSE", "Ergonomic Mouse", new BigDecimal("39.90")),
        new Product("SKU-CABLE", "USB-C Cable", new BigDecimal("12.00")),
    };

    public OrderEvent newOrder(String producerId) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        String orderId = "ord-" + UUID.randomUUID().toString().substring(0, 8);
        String customerId = "cust-" + rnd.nextInt(1, 500);
        String region = REGIONS[rnd.nextInt(REGIONS.length)];

        int lineCount = rnd.nextInt(1, 4);
        List<OrderLine> lines = new ArrayList<>(lineCount);
        BigDecimal total = BigDecimal.ZERO;
        for (int i = 0; i < lineCount; i++) {
            Product p = CATALOG[rnd.nextInt(CATALOG.length)];
            int qty = rnd.nextInt(1, 4);
            OrderLine line = new OrderLine(p.sku(), p.name(), qty, p.price());
            lines.add(line);
            total = total.add(line.lineTotal());
        }

        return new OrderEvent(
            orderId,
            customerId,
            region,
            lines,
            total.setScale(2, RoundingMode.HALF_UP),
            producerId,
            Instant.now()
        );
    }
}
