package com.kafka.demo.case5.consumer.config;

import com.kafka.demo.case5.consumer.exception.PoisonMessageException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * CASE 5: настройка Dead Letter Queue через стандартные средства Spring Kafka.
 *
 * {@link DefaultErrorHandler} + {@link DeadLetterPublishingRecoverer}:
 * - при ошибке обработки сообщение повторяется (retry)
 * - после исчерпания попыток recoverer публикует его в DLQ-топик
 * - имя DLQ-топика задано явно ({@value #DLQ_TOPIC}) — не зависит от версии Spring Kafka
 *
 * Политика повторов:
 * - {@link PoisonMessageException} — non-retryable: сразу в DLQ (повторять бессмысленно)
 * - остальные ошибки — 3 повтора с паузой 1 секунда, затем DLQ
 */
@Slf4j
@Configuration
public class DlqErrorHandlingConfig {

    /** Имя DLQ-топика — задаём явно, чтобы оно не зависело от дефолта Spring Kafka. */
    public static final String DLQ_TOPIC = "orders-dlq-demo.DLT";

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer(
        KafkaTemplate<String, Object> kafkaTemplate
    ) {
        // Явный destination resolver: всё уходит в orders-dlq-demo.DLT, та же партиция
        return new DeadLetterPublishingRecoverer(
            kafkaTemplate,
            (record, exception) -> new TopicPartition(DLQ_TOPIC, record.partition()));
    }

    @Bean
    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
        DefaultErrorHandler handler = new DefaultErrorHandler(
            (record, exception) -> {
                log.error("Sending record to DLQ: topic={} partition={} offset={} cause={}",
                    record.topic(), record.partition(), record.offset(), exception.getMessage());
                recoverer.accept(record, exception);
            },
            new FixedBackOff(1000L, 3)
        );
        // Ядовитые сообщения не повторяем — сразу в DLQ
        handler.addNotRetryableExceptions(PoisonMessageException.class);
        return handler;
    }
}
