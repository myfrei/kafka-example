package com.kafka.demo.case2.consumer.repository;

import com.kafka.demo.case2.consumer.entity.KafkaOffsetEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface KafkaOffsetRepository extends JpaRepository<KafkaOffsetEntity, Long> {

    Optional<KafkaOffsetEntity> findByTopicAndPartitionIdAndConsumerGroup(
        String topic, Integer partitionId, String consumerGroup
    );

    /**
     * Upsert оффсета — атомарная операция:
     * если запись есть — обновляем, если нет — создаём.
     * Использует PostgreSQL-специфичный синтаксис ON CONFLICT.
     */
    @Modifying
    @Query(value = """
        INSERT INTO kafka_offsets (topic, partition_id, offset_value, consumer_group, updated_at)
        VALUES (:topic, :partitionId, :offsetValue, :consumerGroup, NOW())
        ON CONFLICT (topic, partition_id, consumer_group)
        DO UPDATE SET offset_value = :offsetValue, updated_at = NOW()
        """, nativeQuery = true)
    void upsertOffset(String topic, Integer partitionId, Long offsetValue, String consumerGroup);
}
