package com.kafka.demo.case6.consumer.listener;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * CASE 6: Idempotent Consumer — идемпотентная обработка сообщений
 *
 * Даже если продюсер идемпотентен, консьюмер может получить дубли:
 * - Rebalancing прервал обработку до коммита оффсета
 * - Restart сервиса до коммита
 * - At-least-once семантика на стороне консьюмера
 *
 * Решение — дедупликация на стороне консьюмера:
 * - Для каждого сообщения вычисляем уникальный идентификатор
 * - Проверяем в БД — обрабатывали ли мы уже это сообщение
 * - Если да — пропускаем (идемпотентность)
 * - Если нет — обрабатываем и сохраняем ID
 *
 * Идентификатор сообщения: topic + partition + offset (уникален в рамках Kafka)
 *
 * isolation.level=read_committed:
 * - Консьюмер видит ТОЛЬКО сообщения из закоммиченных транзакций
 * - Незакоммиченные (или откатившиеся) транзакции невидимы
 * - Это ключевая настройка для работы с транзакционными продюсерами
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IdempotentConsumerListener {

    private final JdbcTemplate jdbcTemplate;

    @KafkaListener(
        topics = "exactly-once-topic",
        groupId = "exactly-once-group",
        containerFactory = "isolatedListenerContainerFactory"
    )
    @Transactional("transactionManager")  // PostgreSQL транзакция
    public void listen(ConsumerRecord<String, String> record) {
        // Уникальный ID сообщения в Kafka
        String messageId = record.topic() + ":" + record.partition() + ":" + record.offset();

        log.info("Received: messageId={} key={}", messageId, record.key());

        // Проверяем — обрабатывали ли мы уже это сообщение
        if (isAlreadyProcessed(messageId)) {
            log.warn("DUPLICATE DETECTED — skipping: messageId={}", messageId);
            return;
        }

        try {
            // Бизнес-логика
            processMessage(record.value());

            // Сохраняем факт обработки (атомарно с бизнес-данными в одной транзакции)
            markAsProcessed(messageId, record.topic(), record.partition(), record.offset(), record.value());

            log.info("Processed and marked as done: messageId={}", messageId);

        } catch (Exception e) {
            log.error("Processing failed for messageId={}: {}", messageId, e.getMessage());
            throw e; // Откат транзакции — оффсет не сдвинется
        }
    }

    private boolean isAlreadyProcessed(String messageId) {
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM processed_messages WHERE message_id = ?",
            Integer.class,
            messageId
        );
        return count != null && count > 0;
    }

    private void markAsProcessed(String messageId, String topic, int partition, long offset, String payload) {
        jdbcTemplate.update(
            """
            INSERT INTO processed_messages (message_id, topic, partition_id, offset_value, payload, processed_at)
            VALUES (?, ?, ?, ?, ?, NOW())
            ON CONFLICT (message_id) DO NOTHING
            """,
            messageId, topic, partition, offset, payload
        );
    }

    private void processMessage(String value) {
        log.debug("Business logic: {}", value);
        // Здесь: сохранение в orders, вызов внешнего сервиса и т.д.
    }
}
