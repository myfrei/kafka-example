package com.kafka.demo.case6.consumer.config;

import com.kafka.demo.case6.consumer.model.OrderMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Консьюмер с isolation.level=read_committed.
 *
 * Это ключевая настройка для работы с транзакционными продюсерами:
 * консьюмер видит ТОЛЬКО сообщения из закоммиченных Kafka-транзакций.
 * Сообщения откатившихся транзакций невидимы — никаких дублей от откатов.
 */
@Configuration
public class IsolatedConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, OrderMessage> isolatedConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "exactly-once-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // КРИТИЧЕСКИ ВАЖНО для работы с транзакционными продюсерами
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        JsonDeserializer<OrderMessage> valueDeserializer = new JsonDeserializer<>(OrderMessage.class);
        valueDeserializer.setUseTypeHeaders(false);
        valueDeserializer.addTrustedPackages("com.kafka.demo.*");

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderMessage> isolatedListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderMessage> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(isolatedConsumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
