package com.kafka.demo.case1.producer.service;

import com.kafka.demo.case1.producer.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Публикует типизированные события {@link OrderEvent} в топик "orders".
 *
 * Сообщения сериализуются в JSON через JsonSerializer (см. application.yml).
 * Ключ сообщения — orderId: события одного заказа всегда попадут в одну партицию,
 * что сохраняет порядок их обработки.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final OrderFactory orderFactory;

    @Value("${app.kafka.topic}")
    private String topic;

    @Value("${app.producer-id}")
    private String producerId;

    /**
     * Каждые 2 секунды генерируем новый реалистичный заказ.
     * Непрерывный поток позволяет наблюдать, какой консьюмер группы получает событие.
     */
    @Scheduled(fixedDelay = 2000)
    public void sendAutoOrder() {
        publish(orderFactory.newOrder(producerId));
    }

    /**
     * Публикация конкретного заказа (используется REST-контроллером).
     */
    public OrderEvent publish(OrderEvent order) {
        CompletableFuture<SendResult<String, OrderEvent>> future =
            kafkaTemplate.send(topic, order.orderId(), order);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[{}] Failed to send order {}: {}", producerId, order.orderId(), ex.getMessage());
            } else {
                log.info("[{}] Sent order {} → partition={} offset={} total={}",
                    producerId,
                    order.orderId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    order.totalAmount()
                );
            }
        });
        return order;
    }
}
