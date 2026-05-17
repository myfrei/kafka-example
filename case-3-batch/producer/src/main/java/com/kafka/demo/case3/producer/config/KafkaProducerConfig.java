package com.kafka.demo.case3.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Создаёт топик batch-topic с 3 партициями. */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public NewTopic batchTopic() {
        return TopicBuilder.name("batch-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
