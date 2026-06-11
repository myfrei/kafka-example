package com.kafka.demo.case7.consumer.service;

import com.kafka.demo.case7.consumer.chaos.FailureSimulator;
import com.kafka.demo.case7.consumer.exception.PoisonMessageException;
import com.kafka.demo.case7.consumer.model.PaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Бизнес-логика обработки платежа + счётчики для /api/retry-stats.
 *
 * Счётчики и registerDlt() — [ГОТОВО]. Реализовать нужно только классификацию
 * сбоев в process() (TODO 3).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentProcessingService {

    private final FailureSimulator failureSimulator;

    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong dlt = new AtomicLong();

    public void process(PaymentEvent payment) {
        // =================================================================
        // TODO (3): классифицировать сбои перед «успешной» обработкой:
        //   1) poison: если payment.customerId() начинается с "bad-" —
        //        бросить new PoisonMessageException(...) (НЕ повторяемая → сразу в DLT);
        //   2) transient: иначе вызвать failureSimulator.maybeFail("payment " + payment.paymentId())
        //        (случайный повторяемый сбой → уйдёт в retry-топик).
        // Эти два throw — то, что отличает poison от transient в сценарии retry-топиков.
        // =================================================================

        processed.incrementAndGet();
        log.info("Payment {} processed OK (amount={})", payment.paymentId(), payment.amount());
    }

    /** [ГОТОВО] вызывается из @DltHandler */
    public void registerDlt() {
        dlt.incrementAndGet();
    }

    public long processedCount() {
        return processed.get();
    }

    public long dltCount() {
        return dlt.get();
    }
}
