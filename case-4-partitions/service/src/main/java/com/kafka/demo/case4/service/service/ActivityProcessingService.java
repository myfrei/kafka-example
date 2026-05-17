package com.kafka.demo.case4.service.service;

import com.kafka.demo.case4.service.model.ActivityEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Бизнес-логика инстанса сервиса.
 *
 * Считает обработанные события по партициям — по этой статистике видно,
 * что каждый инстанс читает только свои партиции.
 */
@Slf4j
@Service
public class ActivityProcessingService {

    @Value("${app.instance-id}")
    private String instanceId;

    private final Map<Integer, AtomicLong> countByPartition = new ConcurrentHashMap<>();
    private final AtomicLong total = new AtomicLong();

    public void process(ActivityEvent event, int partition) {
        countByPartition.computeIfAbsent(partition, p -> new AtomicLong()).incrementAndGet();
        total.incrementAndGet();
        log.info("[{}] partition={} customer={} type={} seq={} | total={}",
            instanceId, partition, event.customerId(), event.activityType(),
            event.sequenceNo(), total.get());
    }

    public Map<Integer, Long> countByPartition() {
        Map<Integer, Long> snapshot = new ConcurrentHashMap<>();
        countByPartition.forEach((p, c) -> snapshot.put(p, c.get()));
        return snapshot;
    }

    public long total() {
        return total.get();
    }
}
