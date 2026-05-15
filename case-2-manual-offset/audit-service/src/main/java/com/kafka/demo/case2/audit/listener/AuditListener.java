package com.kafka.demo.case2.audit.listener;

import com.kafka.demo.case2.audit.model.OrderEvent;
import com.kafka.demo.case2.audit.service.AuditService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Читает manual-offset-topic в группе case2-audit-group — отдельной от
 * основного консьюмера. Аудит не зависит от ручного управления оффсетами.
 */
@Component
@RequiredArgsConstructor
public class AuditListener {

    private final AuditService auditService;

    @KafkaListener(topics = "manual-offset-topic", groupId = "case2-audit-group")
    public void onOrder(ConsumerRecord<String, OrderEvent> record) {
        auditService.record(record.value());
    }
}
