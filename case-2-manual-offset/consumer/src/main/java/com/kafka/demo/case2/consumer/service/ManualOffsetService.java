package com.kafka.demo.case2.consumer.service;

import com.kafka.demo.case2.consumer.entity.KafkaOffsetEntity;
import com.kafka.demo.case2.consumer.entity.ProcessedOrderEntity;
import com.kafka.demo.case2.consumer.model.OrderEvent;
import com.kafka.demo.case2.consumer.repository.KafkaOffsetRepository;
import com.kafka.demo.case2.consumer.repository.ProcessedOrderRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;

/**
 * Сервис атомарной записи бизнес-данных и Kafka-оффсета в PostgreSQL.
 *
 * Ключевая идея Case 2: оффсет хранится не во внутреннем топике Kafka, а в БД,
 * вместе с бизнес-данными — в одной транзакции. Либо записано всё, либо ничего.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ManualOffsetService {

    public static final String GROUP = "manual-offset-group";

    private final KafkaOffsetRepository offsetRepository;
    private final ProcessedOrderRepository processedOrderRepository;

    /**
     * Атомарно: сохраняет обработанный заказ и сдвигает оффсет в БД.
     */
    @Transactional
    public void persistOrderAndOffset(OrderEvent order, String topic, int partition, long offset) {
        ProcessedOrderEntity processed = processedOrderRepository.findByOrderId(order.orderId())
            .orElseGet(ProcessedOrderEntity::new);
        processed.setOrderId(order.orderId());
        processed.setCustomerId(order.customerId());
        processed.setRegion(order.region());
        processed.setTotalAmount(order.totalAmount());
        processed.setSourcePartition(partition);
        processed.setSourceOffset(offset);
        processed.setProcessedAt(LocalDateTime.now());
        processedOrderRepository.save(processed);

        upsertOffset(topic, partition, offset, GROUP);
        log.info("Persisted order {} and offset {}:{} atomically", order.orderId(), partition, offset);
    }

    /**
     * Find-or-create оффсета — переносимый аналог PostgreSQL upsert (ON CONFLICT).
     */
    @Transactional
    public void upsertOffset(String topic, int partition, long offset, String group) {
        KafkaOffsetEntity entity = offsetRepository
            .findByTopicAndPartitionIdAndConsumerGroup(topic, partition, group)
            .orElseGet(KafkaOffsetEntity::new);
        entity.setTopic(topic);
        entity.setPartitionId(partition);
        entity.setOffsetValue(offset);
        entity.setConsumerGroup(group);
        offsetRepository.save(entity);
    }
}
