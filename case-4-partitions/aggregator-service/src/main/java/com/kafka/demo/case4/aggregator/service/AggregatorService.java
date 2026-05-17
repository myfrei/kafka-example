package com.kafka.demo.case4.aggregator.service;

import com.kafka.demo.case4.aggregator.model.ActivityEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Агрегатор активности.
 *
 * Работает в одном инстансе и в своей consumer group — поэтому видит ВСЕ 3
 * партиции сразу. Это контраст к partition-group: там 3 инстанса делят партиции,
 * здесь один инстанс собирает полную картину.
 */
@Slf4j
@Service
public class AggregatorService {

    private final Map<String, AtomicLong> byType = new ConcurrentHashMap<>();
    private final Map<String, AtomicLong> byRegion = new ConcurrentHashMap<>();

    public void aggregate(ActivityEvent event) {
        byType.computeIfAbsent(event.activityType(), k -> new AtomicLong()).incrementAndGet();
        byRegion.computeIfAbsent(event.region(), k -> new AtomicLong()).incrementAndGet();
        log.info("Aggregated activity: type={} region={} customer={}",
            event.activityType(), event.region(), event.customerId());
    }

    public Map<String, Long> byType() {
        return snapshot(byType);
    }

    public Map<String, Long> byRegion() {
        return snapshot(byRegion);
    }

    private Map<String, Long> snapshot(Map<String, AtomicLong> source) {
        Map<String, Long> result = new ConcurrentHashMap<>();
        source.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }
}
