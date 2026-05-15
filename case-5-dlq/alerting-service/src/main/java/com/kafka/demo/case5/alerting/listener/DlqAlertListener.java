package com.kafka.demo.case5.alerting.listener;

import com.kafka.demo.case5.alerting.model.OrderEvent;
import com.kafka.demo.case5.alerting.service.AlertingService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * Читает orders-dlq-demo.DLT в группе dlq-alerting-group — отдельной от dlq-handler.
 * Один и тот же DLQ-топик читают два независимых потребителя.
 */
@Component
@RequiredArgsConstructor
public class DlqAlertListener {

    private final AlertingService alertingService;

    @KafkaListener(topics = "orders-dlq-demo.DLT", groupId = "dlq-alerting-group")
    public void onDlqMessage(ConsumerRecord<String, OrderEvent> record) {
        Header fqcn = record.headers().lastHeader(KafkaHeaders.DLT_EXCEPTION_FQCN);
        String exceptionType = fqcn == null ? null : new String(fqcn.value(), StandardCharsets.UTF_8);
        OrderEvent order = record.value();
        alertingService.raiseAlert(exceptionType, order != null ? order.orderId() : record.key());
    }
}
