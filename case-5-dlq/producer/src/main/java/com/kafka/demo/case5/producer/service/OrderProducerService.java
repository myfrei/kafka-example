package com.kafka.demo.case5.producer.service;

import com.kafka.demo.case5.producer.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/** Публикует заказы в топик orders-dlq-demo. */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {

    public static final String TOPIC = "orders-dlq-demo";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderEvent publishGood() {
        return publish(new OrderEvent(
            "ord-" + UUID.randomUUID().toString().substring(0, 8),
            "cust-" + ThreadLocalRandom.current().nextInt(1, 500),
            BigDecimal.valueOf(ThreadLocalRandom.current().nextInt(10, 1000)),
            "regular order",
            Instant.now()
        ));
    }

    /** «Ядовитое» сообщение — обработка всегда падает, заказ уйдёт в DLQ. */
    public OrderEvent publishPoison() {
        return publish(new OrderEvent(
            "ord-" + UUID.randomUUID().toString().substring(0, 8),
            "bad-customer",
            BigDecimal.valueOf(666),
            "poison message — will always fail",
            Instant.now()
        ));
    }

    public OrderEvent publish(OrderEvent order) {
        kafkaTemplate.send(TOPIC, order.orderId(), order);
        log.info("Sent order {} (customer={})", order.orderId(), order.customerId());
        return order;
    }
}
