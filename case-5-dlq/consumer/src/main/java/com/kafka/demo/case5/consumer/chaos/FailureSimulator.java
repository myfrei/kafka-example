package com.kafka.demo.case5.consumer.chaos;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Инъекция случайных ВРЕМЕННЫХ сбоев.
 *
 * В отличие от «ядовитых» сообщений, эти ошибки повторяемы: DefaultErrorHandler
 * сделает несколько retry, и часть сообщений обработается успешно со второй попытки.
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
