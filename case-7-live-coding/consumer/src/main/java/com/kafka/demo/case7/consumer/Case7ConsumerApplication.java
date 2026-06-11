package com.kafka.demo.case7.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// ===========================================================================
// TODO (4a) — ВКЛЮЧИТЬ инфраструктуру retry-топиков.
//
//   В Spring Kafka 3.x аннотация @RetryableTopic НЕ работает сама по себе:
//   нужно явно поднять инфраструктуру. Spring Boot её НЕ включает автоматически.
//
//   Добавьте на этот класс аннотацию:
//       @org.springframework.kafka.annotation.EnableKafkaRetryTopic
//   (она @Import-ит RetryTopicConfigurationSupport и включает @EnableKafka).
//
//   Признак, что забыли: после старта НЕ создаются топики
//   payments-retry-demo-retry-0/1/2 и payments-retry-demo-dlt, а poison не уходит в DLT.
// ===========================================================================
@SpringBootApplication
public class Case7ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(Case7ConsumerApplication.class, args);
    }
}
