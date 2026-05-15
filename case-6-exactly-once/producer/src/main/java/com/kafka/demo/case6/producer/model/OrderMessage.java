package com.kafka.demo.case6.producer.model;

import java.time.Instant;

/**
 * Одно сообщение заказа в транзакционном потоке.
 *
 * Заказ публикуется как несколько сообщений: ORDER_HEADER → ORDER_ITEM* → ORDER_FOOTER.
 * Все они отправляются в ОДНОЙ Kafka-транзакции — либо консьюмер увидит все,
 * либо ни одного.
 *
 * @param type          ORDER_HEADER | ORDER_ITEM | ORDER_FOOTER
 * @param orderId       идентификатор заказа
 * @param correlationId единый id транзакции (общий для всех сообщений заказа)
 * @param item          название товара (для ORDER_ITEM)
 * @param sequence      порядковый номер сообщения внутри заказа
 * @param timestamp     момент создания
 */
public record OrderMessage(
    String type,
    String orderId,
    String correlationId,
    String item,
    int sequence,
    Instant timestamp
) {
}
