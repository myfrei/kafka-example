package com.kafka.demo.case3.reporting.listener;

import com.kafka.demo.case3.reporting.model.BatchSummary;
import com.kafka.demo.case3.reporting.service.ReportingService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** Читает сводки батчей из топика batch-summary. */
@Component
@RequiredArgsConstructor
public class BatchSummaryListener {

    private final ReportingService reportingService;

    @KafkaListener(topics = "batch-summary", groupId = "batch-reporting-group")
    public void onSummary(ConsumerRecord<String, BatchSummary> record) {
        reportingService.accept(record.value());
    }
}
