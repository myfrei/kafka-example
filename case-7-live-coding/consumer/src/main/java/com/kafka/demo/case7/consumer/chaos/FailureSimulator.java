package com.kafka.demo.case7.consumer.chaos;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Инъекция случайных ВРЕМЕННЫХ сбоев. [ГОТОВО — не требует изменений]
 *
 * Такие ошибки повторяемы: сообщение уйдёт в retry-топик, и часть платежей
 * обработается успешно на одном из повторов. Не успевшие за все попытки — в DLT.
 */
@Slf4j
@Component
public class FailureSimulator {

    private final double failureRate;

    public FailureSimulator(@Value("${app.chaos.failure-rate:0.0}") double failureRate) {
        this.failureRate = failureRate;
        log.info("FailureSimulator initialised with failure-rate={}", failureRate);
    }

    public void maybeFail(String context) {
        if (failureRate > 0.0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new TransientProcessingException("Chaos: simulated transient failure while processing " + context);
        }
    }
}
