package com.kafka.demo.case4.aggregator.listener;

import com.kafka.demo.case4.aggregator.model.ActivityEvent;
import com.kafka.demo.case4.aggregator.service.AggregatorService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Читает partitioned-topic в группе activity-aggregator-group.
 *
 * concurrency не задаём — один поток читает все 3 партиции.
 */
@Component
@RequiredArgsConstructor
public class ActivityAggregatorListener {

    private final AggregatorService aggregatorService;

    @KafkaListener(topics = "partitioned-topic", groupId = "activity-aggregator-group")
    public void onActivity(ConsumerRecord<String, ActivityEvent> record) {
        aggregatorService.aggregate(record.value());
    }
}
