package com.kafka.demo.case4.service.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * CASE 4: 3 партиции × 3 инстанса сервиса
 *
 * Ключевые концепции:
 *
 * 1. Топик имеет 3 партиции
 * 2. Запущены 3 инстанса этого сервиса, все в group "partition-group"
 * 3. Kafka автоматически назначает каждому инстансу по одной партиции
 *    (правило: число консьюмеров в группе ≤ числу партиций)
 *
 * Результат:
 * - instance-1 читает ТОЛЬКО partition-0
 * - instance-2 читает ТОЛЬКО partition-1
 * - instance-3 читает ТОЛЬКО partition-2
 * → Полный параллелизм, каждая партиция обрабатывается независимо
 *
 * Если запустить 4-й инстанс — он будет idle (больше партиций нет)
 * Если остановить один — его партиция уйдёт к другим (rebalancing)
 *
 * Продюсер использует ключ для детерминированного попадания в партицию:
 * partition = murmur2(key) % numPartitions
 */
@Slf4j
@Component
public class PartitionConsumerListener implements ConsumerSeekAware {

    @Value("${app.instance-id}")
    private String instanceId;

    // Статистика по партициям для этого инстанса
    private final Map<Integer, AtomicLong> messageCountByPartition = new ConcurrentHashMap<>();
    private Set<Integer> assignedPartitions = ConcurrentHashMap.newKeySet();

    @KafkaListener(
        topics = "partitioned-topic",
        groupId = "partition-group"
    )
    public void listen(ConsumerRecord<String, String> record) {
        int partition = record.partition();

        messageCountByPartition
            .computeIfAbsent(partition, k -> new AtomicLong(0))
            .incrementAndGet();

        long totalForPartition = messageCountByPartition.get(partition).get();

        log.info("[{}] partition={} offset={} key={} | total_this_partition={}",
            instanceId,
            partition,
            record.offset(),
            record.key(),
            totalForPartition
        );
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignedPartitions.clear();
        assignments.keySet().forEach(tp -> assignedPartitions.add(tp.partition()));

        log.warn("""
            ╔══════════════════════════════════════════╗
            ║  [{}] PARTITIONS ASSIGNED: {}
            ╚══════════════════════════════════════════╝
            """,
            instanceId,
            assignedPartitions
        );
    }

    @Override
    public void onPartitionsRevoked(java.util.Collection<TopicPartition> partitions) {
        log.warn("[{}] Partitions revoked (rebalancing): {}",
            instanceId,
            partitions.stream().map(tp -> tp.partition()).toList()
        );
    }

    // Статистика доступна через actuator или REST
    public Map<Integer, Long> getStats() {
        Map<Integer, Long> stats = new ConcurrentHashMap<>();
        messageCountByPartition.forEach((p, count) -> stats.put(p, count.get()));
        return stats;
    }
}
