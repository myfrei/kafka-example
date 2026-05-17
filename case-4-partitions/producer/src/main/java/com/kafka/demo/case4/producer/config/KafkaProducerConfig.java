package com.kafka.demo.case4.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Создаёт топик partitioned-topic с 3 партициями. */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public NewTopic partitionedTopic() {
        return TopicBuilder.name("partitioned-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
