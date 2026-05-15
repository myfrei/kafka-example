package com.kafka.demo.case3.producer.service;

import com.kafka.demo.case3.producer.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/** Публикует заказы в топик batch-topic. */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {

    public static final String TOPIC = "batch-topic";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderEvent publish(OrderEvent order) {
        kafkaTemplate.send(TOPIC, order.orderId(), order);
        return order;
    }
}
