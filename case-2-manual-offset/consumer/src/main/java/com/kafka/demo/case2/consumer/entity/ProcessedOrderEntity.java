package com.kafka.demo.case2.consumer.entity;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Бизнес-результат обработки заказа.
 *
 * Сохраняется в той же транзакции, что и Kafka-оффсет ({@link KafkaOffsetEntity}).
 * Это и есть смысл ручного управления оффсетами: бизнес-данные и позиция чтения
 * фиксируются атомарно — либо оба, либо ничего.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "case2_processed_orders")
public class ProcessedOrderEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "order_id", nullable = false, unique = true)
    private String orderId;

    @Column(name = "customer_id")
    private String customerId;

    @Column(name = "region")
    private String region;

    @Column(name = "total_amount", precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Column(name = "source_partition")
    private Integer sourcePartition;

    @Column(name = "source_offset")
    private Long sourceOffset;

    @Column(name = "processed_at", nullable = false)
    private LocalDateTime processedAt;
}
