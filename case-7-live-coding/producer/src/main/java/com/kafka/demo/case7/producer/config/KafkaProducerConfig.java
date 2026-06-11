package com.kafka.demo.case7.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Создаёт ОСНОВНОЙ топик payments-retry-demo (3 партиции).
 *
 * Retry-топики (payments-retry-demo-retry-0/1/2) и DLT (payments-retry-demo-dlt)
 * создаёт сам consumer через @RetryableTopic(autoCreateTopics = "true") — здесь их
 * объявлять не нужно. [ГОТОВО — не требует изменений]
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public NewTopic paymentsRetryDemoTopic() {
        return TopicBuilder.name("payments-retry-demo").partitions(3).replicas(1).build();
    }
}
