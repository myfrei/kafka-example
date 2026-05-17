package com.kafka.demo.case5.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Создаёт основной топик и DLQ-топик (по умолчанию DeadLetterPublishingRecoverer — суффикс .DLT). */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public NewTopic ordersDlqDemoTopic() {
        return TopicBuilder.name("orders-dlq-demo").partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic ordersDlqDemoDltTopic() {
        return TopicBuilder.name("orders-dlq-demo.DLT").partitions(3).replicas(1).build();
    }
}
