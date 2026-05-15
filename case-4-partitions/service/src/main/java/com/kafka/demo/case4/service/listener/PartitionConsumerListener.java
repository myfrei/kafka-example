package com.kafka.demo.case4.service.listener;

import com.kafka.demo.case4.service.chaos.FailureSimulator;
import com.kafka.demo.case4.service.model.ActivityEvent;
import com.kafka.demo.case4.service.service.ActivityProcessingService;
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
 * CASE 4: 3 партиции × 3 инстанса сервиса.
 *
 * Ключевые концепции:
 * 1. Топик partitioned-topic имеет 3 партиции
 * 2. Запущены 3 инстанса сервиса, все в группе partition-group
 * 3. Kafka назначает каждому инстансу по одной партиции — полный параллелизм
 *
 * Что добавлено в реалистичной версии:
 * - В топик приходит типизированный {@link ActivityEvent}, ключ — customerId
 * - {@link FailureSimulator} случайно роняет обработку
 * - Статистика по партициям доступна через REST (см. PartitionStatsController)
 *
 * Если запустить 4-й инстанс — он будет idle (больше партиций нет).
 * Если остановить инстанс — его партиция уйдёт к другим (rebalancing).
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PartitionConsumerListener implements ConsumerSeekAware {

    @Value("${app.instance-id}")
    private String instanceId;

    private final ActivityProcessingService processingService;
    private final FailureSimulator failureSimulator;

    @KafkaListener(topics = "partitioned-topic", groupId = "partition-group")
    public void listen(ConsumerRecord<String, ActivityEvent> record) {
        failureSimulator.maybeFail("activity " + record.key());
        processingService.process(record.value(), record.partition());
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        log.warn("""
            ╔══════════════════════════════════════════╗
            ║  [{}] PARTITIONS ASSIGNED: {}
            ╚══════════════════════════════════════════╝
            """,
            instanceId,
            assignments.keySet().stream().map(TopicPartition::partition).sorted().toList()
        );
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        log.warn("[{}] Partitions revoked (rebalancing): {}",
            instanceId, partitions.stream().map(TopicPartition::partition).sorted().toList());
    }
}
