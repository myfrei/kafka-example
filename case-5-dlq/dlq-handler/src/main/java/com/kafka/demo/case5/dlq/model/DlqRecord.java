package com.kafka.demo.case5.dlq.model;

import java.time.Instant;

/**
 * Запись о сообщении, попавшем в DLQ.
 *
 * Собирается из тела заказа и DLT-заголовков, которые добавил
 * DeadLetterPublishingRecoverer (исходный топик/партиция/оффсет, текст ошибки).
 */
public record DlqRecord(
    String orderId,
    String customerId,
    String originalTopic,
    Integer originalPartition,
    Long originalOffset,
    String exceptionType,
    String exceptionMessage,
    Instant receivedAt
) {
}
