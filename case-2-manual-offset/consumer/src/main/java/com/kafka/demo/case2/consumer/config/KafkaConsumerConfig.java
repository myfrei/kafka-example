package com.kafka.demo.case2.consumer.config;

import com.kafka.demo.case2.consumer.model.OrderEvent;
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
 * Конфигурация Kafka Consumer для Case 2.
 *
 * Ключевые отличия:
 * - enable.auto.commit=false — Kafka не коммитит оффсет автоматически
 * - AckMode.MANUAL_IMMEDIATE — коммит только по явному acknowledge()
 * - значение десериализуется в типизированный {@link OrderEvent} через JsonDeserializer
 */
@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, OrderEvent> manualConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "manual-offset-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);  // Принципиально важно!

        JsonDeserializer<OrderEvent> valueDeserializer = new JsonDeserializer<>(OrderEvent.class);
        valueDeserializer.setUseTypeHeaders(false);
        valueDeserializer.addTrustedPackages("com.kafka.demo.*");

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> manualAckListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory =
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(manualConsumerFactory());
        // MANUAL_IMMEDIATE: acknowledge() сразу коммитит оффсет
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        return factory;
    }
}
