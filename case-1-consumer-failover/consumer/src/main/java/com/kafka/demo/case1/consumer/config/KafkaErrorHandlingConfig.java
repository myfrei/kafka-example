package com.kafka.demo.case1.consumer.config;

import com.kafka.demo.case1.consumer.chaos.TransientProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Обработчик ошибок консьюмера.
 *
 * Spring Boot автоматически подхватывает бин {@link DefaultErrorHandler} и
 * подключает его к авто-сконфигурированной listener-фабрике.
 *
 * Поведение: при {@link TransientProcessingException} сообщение повторяется
 * 3 раза с паузой 1 секунда. Если все попытки исчерпаны — ошибка логируется,
 * оффсет сдвигается дальше (сообщение не блокирует партицию навсегда).
 */
@Slf4j
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        // 3 повторные попытки с интервалом 1 с
        DefaultErrorHandler handler = new DefaultErrorHandler(
            (record, exception) -> log.error(
                "Retries exhausted for record at partition={} offset={} — giving up: {}",
                record.partition(), record.offset(), exception.getMessage()),
            new FixedBackOff(1000L, 3)
        );
        handler.addRetryableExceptions(TransientProcessingException.class);
        return handler;
    }
}
