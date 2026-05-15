package com.kafka.demo.case1.consumer.chaos;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * Инъекция случайных сбоев («chaos»).
 *
 * Реальная обработка сообщений не идеальна: внешние сервисы отваливаются,
 * запросы таймаутятся. Этот компонент с заданной вероятностью бросает
 * {@link TransientProcessingException}, позволяя увидеть, как Kafka и
 * Spring Kafka ведут себя при сбоях (повторная доставка, retry, rebalancing).
 *
 * Вероятность настраивается через app.chaos.failure-rate (0.0 — сбоев нет,
 * 1.0 — падает всегда). В тестах ставится в 0.0 или 1.0 для детерминизма.
 */
@Slf4j
@Component
public class FailureSimulator {

    private final double failureRate;

    public FailureSimulator(@Value("${app.chaos.failure-rate:0.0}") double failureRate) {
        this.failureRate = failureRate;
        log.info("FailureSimulator initialised with failure-rate={}", failureRate);
    }

    /**
     * С вероятностью failureRate бросает {@link TransientProcessingException}.
     *
     * @param context описание обрабатываемого объекта — попадёт в текст ошибки
     */
    public void maybeFail(String context) {
        if (failureRate <= 0.0) {
            return;
        }
        if (ThreadLocalRandom.current().nextDouble() < failureRate) {
            throw new TransientProcessingException("Chaos: simulated transient failure while processing " + context);
        }
    }
}
