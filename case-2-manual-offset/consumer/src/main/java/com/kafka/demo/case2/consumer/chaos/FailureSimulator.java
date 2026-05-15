package com.kafka.demo.case2.consumer.chaos;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Инъекция случайных сбоев обработки.
 *
 * В Case 2 сбой особенно нагляден: если обработка упала, бизнес-данные и оффсет
 * НЕ записываются в PostgreSQL. После перезапуска консьюмер прочитает оффсет из БД
 * (он остался на последнем успешном) и перечитает упавшее сообщение — at-least-once.
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
            throw new TransientProcessingException("Chaos: simulated failure while processing " + context);
        }
    }
}
