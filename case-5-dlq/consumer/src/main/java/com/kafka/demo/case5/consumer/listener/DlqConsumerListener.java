package com.kafka.demo.case5.consumer.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CASE 5: Dead Letter Queue (DLQ)
 *
 * Ключевые концепции:
 *
 * 1. При ошибке обработки — не блокируем весь топик
 * 2. Retry: пробуем N раз с backoff
 * 3. Если все retry исчерпаны → отправляем в DLQ-топик
 * 4. DLQ Handler обрабатывает проблемные сообщения отдельно (алерт, ручная обработка)
 *
 * Топология:
 * [orders-topic] → Consumer → (ошибка? retry 3x) → [orders.DLQ]
 *                                                         ↓
 *                                                    DLQ Handler (алерт / manual fix)
 *
 * Когда нужен DLQ:
 * - Сообщения с невалидным форматом (десериализация упала)
 * - Временно недоступный downstream-сервис (retry exceeded)
 * - Бизнес-правила не выполнены (заказ уже обработан)
 * - Любая непредвиденная ошибка, которую нельзя починить автоматически
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DlqConsumerListener {

    private static final String MAIN_TOPIC = "orders-dlq-demo";
    private static final String DLQ_TOPIC = "orders.DLQ";
    private static final int MAX_RETRIES = 3;

    private final KafkaTemplate<String, String> kafkaTemplate;

    // Счётчик ретраев по ключу сообщения (в реальности — Redis или in-memory Cache)
    private final Map<String, AtomicInteger> retryCounters = new ConcurrentHashMap<>();

    @KafkaListener(
        topics = MAIN_TOPIC,
        groupId = "dlq-consumer-group"
    )
    public void listen(ConsumerRecord<String, String> record) {
        String messageKey = record.key() != null ? record.key() : "null-key-" + record.offset();

        log.info("Processing: partition={} offset={} key={}",
            record.partition(), record.offset(), record.key());

        try {
            processOrder(record.value());

            // Успех — сбрасываем счётчик ретраев
            retryCounters.remove(messageKey);
            log.info("Successfully processed: key={}", record.key());

        } catch (Exception e) {
            handleFailure(record, messageKey, e);
        }
    }

    private void handleFailure(ConsumerRecord<String, String> record, String messageKey, Exception e) {
        AtomicInteger counter = retryCounters.computeIfAbsent(messageKey, k -> new AtomicInteger(0));
        int attempt = counter.incrementAndGet();

        log.warn("Processing failed (attempt {}/{}): key={} error={}",
            attempt, MAX_RETRIES, record.key(), e.getMessage());

        if (attempt >= MAX_RETRIES) {
            // Все попытки исчерпаны → отправляем в DLQ
            sendToDlq(record, e.getMessage());
            retryCounters.remove(messageKey);
        } else {
            // Exponential backoff перед следующей попыткой
            backoff(attempt);
            // В реальности: бросаем исключение, Spring Kafka Retry Backoff сделает это за нас
            throw new RuntimeException("Retry needed: " + e.getMessage(), e);
        }
    }

    private void sendToDlq(ConsumerRecord<String, String> record, String errorMessage) {
        // Добавляем метаданные об ошибке в заголовки
        String dlqValue = String.format(
            "{\"original\": %s, \"error\": \"%s\", \"originalTopic\": \"%s\", " +
            "\"originalPartition\": %d, \"originalOffset\": %d}",
            record.value(), errorMessage, record.topic(), record.partition(), record.offset()
        );

        kafkaTemplate.send(DLQ_TOPIC, record.key(), dlqValue);

        log.error("Message sent to DLQ: key={} topic={} originalOffset={}",
            record.key(), DLQ_TOPIC, record.offset());
    }

    /**
     * Бизнес-логика обработки заказа.
     * Симулирует ошибки для тестирования DLQ:
     * - Ключи, начинающиеся с "fail-" → всегда падают
     * - Ключи, начинающиеся с "flaky-" → падают 2 из 3 раз (тест retry)
     */
    private void processOrder(String orderJson) {
        if (orderJson == null || orderJson.contains("\"invalid\"")) {
            throw new IllegalArgumentException("Invalid order format");
        }
        if (orderJson.contains("\"fail\"")) {
            throw new RuntimeException("Simulated permanent failure");
        }
        log.debug("Order processed successfully: {}", orderJson);
    }

    private void backoff(int attempt) {
        try {
            long delay = (long) Math.pow(2, attempt) * 100; // 200ms, 400ms, ...
            log.debug("Backoff for {}ms (attempt {})", delay, attempt);
            Thread.sleep(delay);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        }
    }
}
