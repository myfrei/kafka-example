package com.kafka.demo.case7.producer.service;

import com.kafka.demo.case7.producer.model.PaymentEvent;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Фабрика платежей. [ГОТОВО — не требует изменений]
 *
 * newPayment()        — обычный платёж (обрабатывается, иногда падает с transient-сбоем);
 * newPoisonPayment()  — «ядовитый» платёж (customerId="bad-customer") → должен уйти в DLT.
 */
@Component
public class PaymentFactory {

    public PaymentEvent newPayment() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return new PaymentEvent(
            "pay-" + UUID.randomUUID().toString().substring(0, 8),
            "ord-" + rnd.nextInt(1, 9999),
            "cust-" + rnd.nextInt(1, 200),
            BigDecimal.valueOf(rnd.nextInt(10, 5000)),
            Instant.now()
        );
    }

    public PaymentEvent newPoisonPayment() {
        return new PaymentEvent(
            "pay-" + UUID.randomUUID().toString().substring(0, 8),
            "ord-" + ThreadLocalRandom.current().nextInt(1, 9999),
            "bad-customer",
            BigDecimal.valueOf(666),
            Instant.now()
        );
    }
}
