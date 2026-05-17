package com.kafka.demo.case4.producer.model;

import java.time.Instant;

/**
 * Событие активности покупателя.
 *
 * Ключ сообщения — customerId: все события одного покупателя детерминированно
 * попадают в одну партицию (partition = murmur2(key) % numPartitions),
 * а значит, обрабатываются по порядку одним инстансом сервиса.
 *
 * @param customerId   идентификатор покупателя (ключ сообщения)
 * @param activityType тип события: VIEW / ADD_TO_CART / CHECKOUT / LOGIN
 * @param region       регион
 * @param sequenceNo   порядковый номер события покупателя
 * @param timestamp    момент события
 */
public record ActivityEvent(
    String customerId,
    String activityType,
    String region,
    int sequenceNo,
    Instant timestamp
) {
}
