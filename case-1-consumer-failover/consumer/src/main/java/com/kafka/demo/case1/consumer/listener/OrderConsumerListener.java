package com.kafka.demo.case1.consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * CASE 1: Consumer Failover
 *
 * Демонстрирует поведение Kafka при потере одного из консьюмеров в группе.
 *
 * Ключевые концепции:
 * - Consumer Group: все инстансы с одним group-id читают топик совместно
 * - Partition Assignment: Kafka делит партиции между консьюмерами в группе
 * - Rebalancing: при падении консьюмера его партиции перераспределяются между живыми
 *
 * Сценарий проверки:
 * 1. Запустить 2 инстанса — каждый получит ~1-2 партиции из 3
 * 2. Остановить consumer-2 (docker stop case1-consumer-2)
 * 3. Наблюдать: consumer-1 начнёт получать сообщения из ВСЕХ партиций
 * 4. Снова запустить consumer-2 — произойдёт rebalancing, партиции перераспределятся
 */
@Slf4j
@Component
public class OrderConsumerListener implements ConsumerSeekAware {

    @Value("${app.consumer-instance-id}")
    private String instanceId;

    /**
     * Слушаем топик orders.
     * containerFactory не указан — используется defaultKafkaListenerContainerFactory.
     * concurrency не указан — один поток на listener (достаточно для демо).
     */
    @KafkaListener(
        topics = "orders",
        groupId = "order-group"
    )
    public void listen(ConsumerRecord<String, String> record) {
        log.info("""
            [{}] RECEIVED MESSAGE
              Topic:     {}
              Partition: {}
              Offset:    {}
              Key:       {}
              Value:     {}
            """,
            instanceId,
            record.topic(),
            record.partition(),
            record.offset(),
            record.key(),
            record.value()
        );

        // Симулируем обработку
        processOrder(record.value());
    }

    private void processOrder(String orderJson) {
        // В реальном приложении здесь была бы бизнес-логика
        log.debug("[{}] Processing order: {}", instanceId, orderJson);
    }

    /**
     * Этот callback вызывается при каждом rebalancing.
     * Позволяет увидеть, какие партиции назначены данному инстансу.
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.warn("""
            ============================================
            [{}] REBALANCING COMPLETE — Partitions assigned: {}
            ============================================
            """,
            instanceId,
            assignments.keySet().stream()
                .map(tp -> "partition-" + tp.partition())
                .toList()
        );
    }

    @Override
    public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
        log.warn("[{}] REBALANCING START — Partitions revoked: {}",
            instanceId,
            partitions.stream().map(tp -> "partition-" + tp.partition()).toList()
        );
    }
}
