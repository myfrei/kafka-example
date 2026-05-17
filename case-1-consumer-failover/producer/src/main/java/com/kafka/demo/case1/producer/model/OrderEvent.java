package com.kafka.demo.case1.producer.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Событие заказа — типизированная доменная модель, которая публикуется в топик "orders".
 *
 * Раньше кейс отправлял в Kafka «сырую» строку JSON. Теперь продюсер сериализует
 * полноценный объект через JsonSerializer, а консьюмер десериализует его обратно
 * в типизированный объект — это ближе к реальным системам.
 *
 * @param orderId     уникальный идентификатор заказа (используется как ключ сообщения)
 * @param customerId  идентификатор покупателя
 * @param region      регион доставки — по нему analytics-service агрегирует выручку
 * @param lines       список позиций заказа
 * @param totalAmount итоговая сумма заказа
 * @param producerId  какой инстанс продюсера создал событие
 * @param createdAt   момент создания события
 */
public record OrderEvent(
    String orderId,
    String customerId,
    String region,
    List<OrderLine> lines,
    BigDecimal totalAmount,
    String producerId,
    Instant createdAt
) {
}
