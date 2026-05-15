package com.kafka.demo.case5.consumer.service;

import com.kafka.demo.case5.consumer.chaos.FailureSimulator;
import com.kafka.demo.case5.consumer.exception.PoisonMessageException;
import com.kafka.demo.case5.consumer.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Бизнес-логика обработки заказа.
 *
 * Сбои двух видов:
 * - {@link PoisonMessageException} — детерминированный, для покупателей "bad-*"
 * - transient (через {@link FailureSimulator}) — случайный, повторяемый
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProcessingService {

    private final FailureSimulator failureSimulator;

    private final AtomicLong processed = new AtomicLong();

    public void process(OrderEvent order) {
        if (order.customerId() != null && order.customerId().startsWith("bad-")) {
            throw new PoisonMessageException(
                "Poison order " + order.orderId() + " — customer " + order.customerId());
        }
        failureSimulator.maybeFail("order " + order.orderId());

        processed.incrementAndGet();
        log.info("Order {} processed successfully (amount={})", order.orderId(), order.amount());
    }

    public long processedCount() {
        return processed.get();
    }
}
