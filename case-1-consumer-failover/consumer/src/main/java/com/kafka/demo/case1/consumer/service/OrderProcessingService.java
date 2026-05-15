package com.kafka.demo.case1.consumer.service;

import com.kafka.demo.case1.consumer.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Бизнес-логика обработки заказа на стороне консьюмера.
 *
 * Хранит in-memory состояние об обработанных заказах — это даёт наблюдаемый
 * результат: по нему строится статистика инстанса и проверяются интеграционные тесты.
 */
@Slf4j
@Service
public class OrderProcessingService {

    /** orderId -> регион обработанного заказа. */
    private final Map<String, String> processedOrders = new ConcurrentHashMap<>();
    private final AtomicLong processedCount = new AtomicLong();

    public void process(OrderEvent order) {
        log.info("Processing order {} (customer={}, region={}, total={}, lines={})",
            order.orderId(), order.customerId(), order.region(),
            order.totalAmount(), order.lines().size());
        processedOrders.put(order.orderId(), order.region());
        processedCount.incrementAndGet();
    }

    public boolean wasProcessed(String orderId) {
        return processedOrders.containsKey(orderId);
    }

    public long processedCount() {
        return processedCount.get();
    }

    public int distinctOrderCount() {
        return processedOrders.size();
    }
}
