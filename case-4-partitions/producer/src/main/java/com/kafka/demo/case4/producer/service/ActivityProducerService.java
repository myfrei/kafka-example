package com.kafka.demo.case4.producer.service;

import com.kafka.demo.case4.producer.model.ActivityEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/** Публикует события активности в топик partitioned-topic. */
@Slf4j
@Service
@RequiredArgsConstructor
public class ActivityProducerService {

    public static final String TOPIC = "partitioned-topic";

    private final KafkaTemplate<String, ActivityEvent> kafkaTemplate;

    /** Отправка с ключом — партиция выбирается по customerId. */
    public void publish(ActivityEvent event) {
        kafkaTemplate.send(TOPIC, event.customerId(), event)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Sent activity for {} → partition={}",
                        event.customerId(), result.getRecordMetadata().partition());
                }
            });
    }

    /** Отправка с явным указанием партиции (в обход key-routing). */
    public void publishToPartition(int partition, ActivityEvent event) {
        kafkaTemplate.send(TOPIC, partition, event.customerId(), event);
        log.info("Sent activity for {} → forced partition={}", event.customerId(), partition);
    }
}
