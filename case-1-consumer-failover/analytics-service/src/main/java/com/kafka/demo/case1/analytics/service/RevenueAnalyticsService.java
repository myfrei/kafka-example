package com.kafka.demo.case1.analytics.service;

import com.kafka.demo.case1.analytics.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Накапливает выручку по регионам.
 *
 * Это отдельный потребитель в СВОЕЙ consumer group — поэтому он получает
 * все события "orders" целиком, независимо от того, как failover делит
 * партиции между инстансами order-processing-group.
 */
@Slf4j
@Service
public class RevenueAnalyticsService {

    private final Map<String, BigDecimal> revenueByRegion = new ConcurrentHashMap<>();
    private final Map<String, Long> ordersByRegion = new ConcurrentHashMap<>();

    public void register(OrderEvent order) {
        revenueByRegion.merge(order.region(), order.totalAmount(), BigDecimal::add);
        ordersByRegion.merge(order.region(), 1L, Long::sum);
        log.info("Analytics: region={} order={} total={} | regionRevenue={}",
            order.region(), order.orderId(), order.totalAmount(), revenueByRegion.get(order.region()));
    }

    public Map<String, BigDecimal> revenueByRegion() {
        return Map.copyOf(revenueByRegion);
    }

    public Map<String, Long> ordersByRegion() {
        return Map.copyOf(ordersByRegion);
    }
}
