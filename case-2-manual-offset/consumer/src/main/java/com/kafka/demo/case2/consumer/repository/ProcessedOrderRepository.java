package com.kafka.demo.case2.consumer.repository;

import com.kafka.demo.case2.consumer.entity.ProcessedOrderEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ProcessedOrderRepository extends JpaRepository<ProcessedOrderEntity, Long> {

    Optional<ProcessedOrderEntity> findByOrderId(String orderId);
}
