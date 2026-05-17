package com.kafka.demo.case6.shipping.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Downstream-сервис доставки.
 *
 * Читает exactly-once-topic с isolation.level=read_committed — поэтому видит
 * только заказы из закоммиченных транзакций. Заказ считается готовым к отгрузке,
 * когда пришёл его ORDER_FOOTER (все сообщения заказа доставлены атомарно).
 */
@Slf4j
@Service
public class ShippingService {

    private final List<String> shippedOrders = new CopyOnWriteArrayList<>();

    public void ship(String orderId) {
        shippedOrders.add(orderId);
        log.info("Shipping order {} — total shipped: {}", orderId, shippedOrders.size());
    }

    public List<String> shippedOrders() {
        return List.copyOf(shippedOrders);
    }

    public int shippedCount() {
        return shippedOrders.size();
    }
}
