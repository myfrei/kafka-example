package com.kafka.demo.case6.consumer.service;

import com.kafka.demo.case6.consumer.chaos.FailureSimulator;
import com.kafka.demo.case6.consumer.model.OrderMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * Идемпотентная обработка сообщений с дедупликацией в PostgreSQL.
 *
 * Даже при exactly-once на стороне продюсера консьюмер может получить дубль
 * (rebalancing/restart до коммита оффсета). Защита — дедупликация:
 * уникальный messageId = topic:partition:offset, проверка в processed_messages.
 *
 * Запись факта обработки и бизнес-данных идёт в ОДНОЙ транзакции БД:
 * либо зафиксировано всё, либо ничего (а значит, сообщение перечитается).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotentOrderService {

    private final JdbcTemplate jdbcTemplate;
    private final FailureSimulator failureSimulator;

    /**
     * @return true — сообщение обработано впервые; false — это дубль, пропущен
     */
    @Transactional("transactionManager")
    public boolean processIfNew(String messageId, OrderMessage message,
                                String topic, int partition, long offset) {
        if (isAlreadyProcessed(messageId)) {
            log.warn("DUPLICATE detected — skipping messageId={}", messageId);
            return false;
        }

        // Сбой откатит всю транзакцию — processed_messages не пополнится
        failureSimulator.maybeFail("messageId " + messageId);

        Timestamp now = Timestamp.from(Instant.now());
        jdbcTemplate.update(
            "INSERT INTO processed_messages (message_id, topic, partition_id, offset_value, payload, processed_at) "
                + "VALUES (?, ?, ?, ?, ?, ?)",
            messageId, topic, partition, offset, describe(message), now);

        // Бизнес-данные: заголовок заказа создаёт строку в orders
        if ("ORDER_HEADER".equals(message.type()) && !orderExists(message.orderId())) {
            jdbcTemplate.update(
                "INSERT INTO orders (order_id, status, created_at) VALUES (?, ?, ?)",
                message.orderId(), "RECEIVED", now);
        }

        log.info("Processed messageId={} type={} order={}",
            messageId, message.type(), message.orderId());
        return true;
    }

    private boolean isAlreadyProcessed(String messageId) {
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM processed_messages WHERE message_id = ?", Integer.class, messageId);
        return count != null && count > 0;
    }

    private boolean orderExists(String orderId) {
        Integer count = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM orders WHERE order_id = ?", Integer.class, orderId);
        return count != null && count > 0;
    }

    private String describe(OrderMessage m) {
        return m.type() + (m.item() != null ? " item=" + m.item() : "");
    }
}
