package com.kafka.demo.case2.audit.service;

import com.kafka.demo.case2.audit.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Журнал аудита: фиксирует факт появления каждого заказа в топике.
 *
 * Audit-service читает топик в собственной consumer group, поэтому видит ВСЕ
 * заказы независимо от того, как основной консьюмер управляет своими оффсетами
 * (и независимо от его replay/сбоев).
 */
@Slf4j
@Service
public class AuditService {

    public record AuditRecord(String orderId, String region, Instant seenAt) {}

    private static final int MAX_KEPT = 200;
    private final Deque<AuditRecord> trail = new ConcurrentLinkedDeque<>();
    private final AtomicLong total = new AtomicLong();

    public void record(OrderEvent order) {
        trail.addFirst(new AuditRecord(order.orderId(), order.region(), Instant.now()));
        while (trail.size() > MAX_KEPT) {
            trail.pollLast();
        }
        total.incrementAndGet();
        log.info("Audited order {} (region={})", order.orderId(), order.region());
    }

    public List<AuditRecord> recent() {
        return List.copyOf(trail);
    }

    public long totalAudited() {
        return total.get();
    }
}
