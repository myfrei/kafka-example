package com.kafka.demo.case6.producer.service;

import com.kafka.demo.case6.producer.model.OrderMessage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * CASE 6: Exactly-Once Semantics (EOS) — транзакционный продюсер.
 *
 * Заказ публикуется как несколько сообщений (header + items + footer) в ОДНОЙ
 * Kafka-транзакции. Либо консьюмер с isolation.level=read_committed увидит все,
 * либо (при откате) — ни одного.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionalProducerService {

    private static final String TOPIC = "exactly-once-topic";

    private final KafkaTemplate<String, OrderMessage> kafkaTemplate;

    /**
     * Транзакционная отправка заказа: header + по сообщению на каждый item + footer.
     * При выходе из метода Spring Kafka автоматически вызовет commitTransaction().
     */
    @Transactional("kafkaTransactionManager")
    public String sendOrderWithItems(String orderId, List<String> items) {
        String correlationId = UUID.randomUUID().toString();
        log.info("Starting Kafka transaction for orderId={} correlationId={}", orderId, correlationId);

        send(new OrderMessage("ORDER_HEADER", orderId, correlationId, null, 0, Instant.now()));
        for (int i = 0; i < items.size(); i++) {
            send(new OrderMessage("ORDER_ITEM", orderId, correlationId, items.get(i), i + 1, Instant.now()));
        }
        send(new OrderMessage("ORDER_FOOTER", orderId, correlationId, null, items.size() + 1, Instant.now()));

        log.info("All {} messages staged for orderId={}", items.size() + 2, orderId);
        return correlationId;
    }

    /**
     * Демонстрация отката: отправляем сообщения, затем бросаем исключение.
     * Консьюмер с read_committed НЕ увидит ни одного из этих сообщений.
     */
    @Transactional("kafkaTransactionManager")
    public void sendAndFail(String orderId) {
        send(new OrderMessage("ORDER_HEADER", orderId, UUID.randomUUID().toString(), null, 0, Instant.now()));
        send(new OrderMessage("ORDER_ITEM", orderId, "rollback", "will-not-be-seen", 1, Instant.now()));
        log.warn("About to throw — transaction will ROLLBACK, messages won't be visible!");
        throw new IllegalStateException("Simulated failure — transaction rolled back");
    }

    private void send(OrderMessage message) {
        kafkaTemplate.send(TOPIC, message.orderId(), message);
        log.info("Staged {} for order {}", message.type(), message.orderId());
    }
}
