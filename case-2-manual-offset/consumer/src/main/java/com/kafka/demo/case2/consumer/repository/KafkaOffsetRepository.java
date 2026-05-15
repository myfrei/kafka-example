package com.kafka.demo.case2.consumer.repository;

import com.kafka.demo.case2.consumer.entity.KafkaOffsetEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Репозиторий Kafka-оффсетов.
 *
 * Upsert делается в сервисном слое (find-or-create) — это переносимо между
 * PostgreSQL и H2 (используется в тестах) и не зависит от диалекта SQL.
 */
@Repository
public interface KafkaOffsetRepository extends JpaRepository<KafkaOffsetEntity, Long> {

    Optional<KafkaOffsetEntity> findByTopicAndPartitionIdAndConsumerGroup(
        String topic, Integer partitionId, String consumerGroup
    );
}
