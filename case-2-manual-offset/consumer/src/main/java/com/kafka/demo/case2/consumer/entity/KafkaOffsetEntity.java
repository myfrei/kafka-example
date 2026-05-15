package com.kafka.demo.case2.consumer.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Хранит Kafka-оффсеты в PostgreSQL.
 *
 * Зачем это нужно?
 * - Стандартные оффсеты Kafka хранятся во внутреннем топике __consumer_offsets
 * - Иногда нужно хранить оффсеты в своей БД для:
 *   1. Атомарной записи оффсета + бизнес-данных в одной транзакции
 *   2. Возможности откатить оффсет (replay) без административных инструментов
 *   3. Хранения более детального контекста (кто обработал, когда, с каким результатом)
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "kafka_offsets",
    uniqueConstraints = @UniqueConstraint(columnNames = {"topic", "partition_id", "consumer_group"}))
public class KafkaOffsetEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String topic;

    @Column(name = "partition_id", nullable = false)
    private Integer partitionId;

    @Column(name = "offset_value", nullable = false)
    private Long offsetValue;

    @Column(name = "consumer_group", nullable = false)
    private String consumerGroup;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    @PreUpdate
    public void setUpdatedAt() {
        this.updatedAt = LocalDateTime.now();
    }
}
