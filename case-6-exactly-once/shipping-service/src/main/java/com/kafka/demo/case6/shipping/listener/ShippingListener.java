package com.kafka.demo.case6.shipping.listener;

import com.kafka.demo.case6.shipping.model.OrderMessage;
import com.kafka.demo.case6.shipping.service.ShippingService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Читает exactly-once-topic в группе shipping-group.
 *
 * Отгрузка инициируется по ORDER_FOOTER — это значит, что вся транзакция заказа
 * закоммичена и видна (isolation.level=read_committed настроен в application.yml).
 */
@Component
@RequiredArgsConstructor
public class ShippingListener {

    private final ShippingService shippingService;

    @KafkaListener(topics = "exactly-once-topic", groupId = "shipping-group")
    public void onMessage(ConsumerRecord<String, OrderMessage> record) {
        OrderMessage message = record.value();
        if (message != null && "ORDER_FOOTER".equals(message.type())) {
            shippingService.ship(message.orderId());
        }
    }
}
