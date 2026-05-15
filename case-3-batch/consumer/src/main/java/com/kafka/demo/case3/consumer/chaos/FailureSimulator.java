package com.kafka.demo.case3.consumer.chaos;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Инъекция случайных сбоев на уровне ЦЕЛОГО батча.
 *
 * Если батч падает — он не коммитится целиком (риск батч-обработки: всё или ничего).
 * Поэтому бизнес-логика обязана быть идемпотентной.
 */
@Slf4j
@Component
public class FailureSimulator {

    private final double failureRate;

    public FailureSimulator(@Value("${app.chaos.failure-rate:0.0}") double failureRate) {
        this.failureRate = failureRate;
        log.info("FailureSimulator initialised with failure-rate={}", failureRate);
    }

    public void maybeFailBatch(int batchSize) {
        if (failureRate > 0.0 && ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new TransientProcessingException("Chaos: simulated failure for batch of " + batchSize);
        }
    }
}
