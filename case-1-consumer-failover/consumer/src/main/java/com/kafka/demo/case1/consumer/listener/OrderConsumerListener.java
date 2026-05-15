package com.kafka.demo.case1.consumer.listener;

import com.kafka.demo.case1.consumer.chaos.FailureSimulator;
import com.kafka.demo.case1.consumer.model.OrderEvent;
import com.kafka.demo.case1.consumer.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Collection;
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
 * Что добавлено в реалистичной версии:
 * - В топик приходит типизированный {@link OrderEvent}, а не сырая строка
 * - {@link FailureSimulator} случайно роняет обработку — видно повторную доставку
 *   (см. DefaultErrorHandler в KafkaErrorHandlingConfig)
 *
 * Сценарий проверки:
 * 1. Запустить 2 инстанса — каждый получит ~1-2 партиции из 3
 * 2. Остановить consumer-2 (docker stop case1-consumer-2)
 * 3. Наблюдать: consumer-1 начнёт получать сообщения из ВСЕХ партиций
 * 4. Снова запустить consumer-2 — произойдёт rebalancing, партиции перераспределятся
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class OrderConsumerListener implements ConsumerSeekAware {

    @Value("${app.consumer-instance-id}")
    private String instanceId;

    private final OrderProcessingService processingService;
    private final FailureSimulator failureSimulator;

    @KafkaListener(topics = "orders", groupId = "order-processing-group")
    public void listen(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent order = record.value();
        log.info("[{}] RECEIVED order={} partition={} offset={}",
            instanceId, order.orderId(), record.partition(), record.offset());

        // Инъекция случайного сбоя — transient-ошибку повторит DefaultErrorHandler
        failureSimulator.maybeFail("order " + order.orderId());

        processingService.process(order);
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.warn("""
            ============================================
            [{}] REBALANCING COMPLETE — Partitions assigned: {}
            ============================================
            """,
            instanceId,
            assignments.keySet().stream().map(tp -> "partition-" + tp.partition()).toList()
        );
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.warn("[{}] REBALANCING START — Partitions revoked: {}",
            instanceId,
            partitions.stream().map(tp -> "partition-" + tp.partition()).toList()
        );
    }
}
