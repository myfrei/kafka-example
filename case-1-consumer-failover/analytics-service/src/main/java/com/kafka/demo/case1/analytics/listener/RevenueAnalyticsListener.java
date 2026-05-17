package com.kafka.demo.case1.analytics.listener;

import com.kafka.demo.case1.analytics.model.OrderEvent;
import com.kafka.demo.case1.analytics.service.RevenueAnalyticsService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Читает топик "orders" в отдельной consumer group "order-analytics-group".
 *
 * Зачем отдельная группа: показывает, что один и тот же топик можно читать
 * несколькими независимыми потребителями. Failover внутри order-processing-group
 * никак не влияет на аналитику — у неё свои оффсеты и своё распределение партиций.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RevenueAnalyticsListener {

    private final RevenueAnalyticsService analyticsService;

    @KafkaListener(topics = "orders", groupId = "order-analytics-group")
    public void onOrder(ConsumerRecord<String, OrderEvent> record) {
        analyticsService.register(record.value());
    }
}
