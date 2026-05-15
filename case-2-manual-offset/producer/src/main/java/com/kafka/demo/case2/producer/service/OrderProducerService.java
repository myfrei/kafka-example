package com.kafka.demo.case2.producer.service;

import com.kafka.demo.case2.producer.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/** Публикует заказы в топик manual-offset-topic. */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {

    public static final String TOPIC = "manual-offset-topic";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderEvent publish(OrderEvent order) {
        kafkaTemplate.send(TOPIC, order.orderId(), order)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to send order {}: {}", order.orderId(), ex.getMessage());
                } else {
                    log.info("Sent order {} → partition={} offset={}",
                        order.orderId(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
                }
            });
        return order;
    }
}
