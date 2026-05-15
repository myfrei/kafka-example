package com.kafka.demo.case6.producer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

/**
 * CASE 6: Exactly-Once Semantics (EOS) — Транзакционный продюсер
 *
 * Ключевые концепции:
 *
 * 1. Idempotent Producer (enable.idempotence=true):
 *    - Каждое сообщение получает уникальный sequence number
 *    - Broker отклоняет дубликаты (ретри продюсера не создадут дублей)
 *    - Гарантия: exactly-once на уровне продюсер → broker
 *
 * 2. Transactional Producer (transactional.id):
 *    - Несколько сообщений отправляются атомарно
 *    - Либо ВСЕ сохраняются → либо НИ ОДНО
 *    - Консьюмер с isolation.level=read_committed видит только закоммиченные транзакции
 *
 * 3. @Transactional + KafkaTransactionManager:
 *    - Spring автоматически оборачивает отправку в Kafka-транзакцию
 *    - Откат транзакции = откат всех отправленных в неё сообщений
 *
 * Ограничения:
 * - Транзакционный режим снижает throughput (coordination overhead)
 * - transactional.id должен быть уникальным на каждый инстанс продюсера
 * - Нужен Kafka 0.11+ (ABI incompatible со старыми версиями)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TransactionalProducerService {

    private static final String TOPIC = "exactly-once-topic";
    private final KafkaTemplate<String, String> kafkaTemplate;

    /**
     * Транзакционная отправка: все сообщения попадут в топик атомарно.
     *
     * Если метод бросит исключение — транзакция откатится,
     * и ни одно из сообщений не будет видно консьюмерам с read_committed.
     */
    @Transactional("kafkaTransactionManager")
    public String sendOrderWithItems(String orderId, List<String> items) {
        String correlationId = UUID.randomUUID().toString();

        log.info("Starting Kafka transaction for orderId={} correlationId={}", orderId, correlationId);

        // Отправляем заголовок заказа
        String orderHeader = String.format(
            "{\"type\":\"ORDER_HEADER\", \"orderId\":\"%s\", \"correlationId\":\"%s\", \"itemCount\":%d}",
            orderId, correlationId, items.size()
        );
        kafkaTemplate.send(TOPIC, orderId, orderHeader);
        log.info("Sent ORDER_HEADER: orderId={}", orderId);

        // Отправляем каждый item заказа
        for (int i = 0; i < items.size(); i++) {
            String itemKey = orderId + "-item-" + i;
            String itemMessage = String.format(
                "{\"type\":\"ORDER_ITEM\", \"orderId\":\"%s\", \"item\":\"%s\", \"seq\":%d, \"correlationId\":\"%s\"}",
                orderId, items.get(i), i, correlationId
            );
            kafkaTemplate.send(TOPIC, itemKey, itemMessage);
            log.info("Sent ORDER_ITEM[{}]: {}", i, items.get(i));
        }

        // Отправляем footer
        String orderFooter = String.format(
            "{\"type\":\"ORDER_FOOTER\", \"orderId\":\"%s\", \"correlationId\":\"%s\"}",
            orderId, correlationId
        );
        kafkaTemplate.send(TOPIC, orderId + "-footer", orderFooter);

        log.info("All {} messages staged in transaction for orderId={}", items.size() + 2, orderId);
        return correlationId;
        // При выходе из метода Spring Kafka автоматически вызовет commitTransaction()
    }

    /**
     * Демонстрирует откат: отправляем несколько сообщений, потом бросаем исключение.
     * Консьюмер с isolation.level=read_committed НЕ увидит ни одного из этих сообщений.
     */
    @Transactional("kafkaTransactionManager")
    public void sendAndFail(String orderId) {
        kafkaTemplate.send(TOPIC, orderId, "{\"type\":\"WILL_ROLLBACK\", \"orderId\":\"" + orderId + "\"}");
        kafkaTemplate.send(TOPIC, orderId + "-2", "{\"type\":\"WILL_ROLLBACK_TOO\"}");

        log.warn("About to throw exception — transaction will ROLLBACK, messages won't be visible!");
        throw new RuntimeException("Simulated failure — transaction rolled back");
    }
}
