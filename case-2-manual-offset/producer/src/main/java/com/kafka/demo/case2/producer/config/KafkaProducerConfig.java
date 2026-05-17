package com.kafka.demo.case2.producer.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.apache.kafka.clients.admin.NewTopic;

/**
 * Создаёт топик manual-offset-topic с 3 партициями.
 */
@Configuration
public class KafkaProducerConfig {

    @Bean
    public NewTopic manualOffsetTopic() {
        return TopicBuilder.name("manual-offset-topic")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
