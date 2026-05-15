package com.kafka.demo.case6.consumer.chaos;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Инъекция случайных сбоев.
 *
 * Сбой откатывает транзакцию БД (запись в processed_messages не фиксируется),
 * оффсет не коммитится — сообщение будет доставлено повторно. При повторе
 * дедупликация по message_id может как сработать (если предыдущая попытка
 * успела закоммититься), так и нет — это и проверяет идемпотентность.
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
