package com.kafka.demo.case5.alerting.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Сервис алертинга по DLQ.
 *
 * Второй независимый потребитель DLQ-топика (своя consumer group): пока
 * dlq-handler сохраняет сообщения для разбора, alerting-service считает
 * ошибки по типу и «поднимает тревогу» (в реальности — Slack/PagerDuty).
 */
@Slf4j
@Service
public class AlertingService {

    private final Map<String, AtomicLong> alertsByException = new ConcurrentHashMap<>();
    private final AtomicLong totalAlerts = new AtomicLong();

    public void raiseAlert(String exceptionType, String orderId) {
        String type = exceptionType == null ? "UNKNOWN" : exceptionType;
        alertsByException.computeIfAbsent(type, k -> new AtomicLong()).incrementAndGet();
        long total = totalAlerts.incrementAndGet();
        log.warn("ALERT #{}: order {} failed permanently — {}", total, orderId, type);
    }

    public Map<String, Long> alertsByException() {
        Map<String, Long> snapshot = new ConcurrentHashMap<>();
        alertsByException.forEach((k, v) -> snapshot.put(k, v.get()));
        return snapshot;
    }

    public long totalAlerts() {
        return totalAlerts.get();
    }
}
