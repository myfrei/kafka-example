package com.kafka.demo.case6.producer.config;

import com.kafka.demo.case6.producer.model.OrderMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Конфигурация транзакционного (идемпотентного) продюсера.
 *
 * Отличие от обычного продюсера:
 * 1. enable.idempotence=true — дедупликация на уровне broker
 * 2. transactional.id — включает транзакционный режим
 * 3. acks=all — обязательно для идемпотентного продюсера
 * 4. значение сериализуется в JSON (типизированный {@link OrderMessage})
 */
@Configuration
public class TransactionalKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, OrderMessage> transactionalProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        // --- Idempotence ---
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        DefaultKafkaProducerFactory<String, OrderMessage> factory =
            new DefaultKafkaProducerFactory<>(props);
        // transactionIdPrefix включает транзакционный режим продюсера
        factory.setTransactionIdPrefix("case6-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, OrderMessage> kafkaTemplate() {
        return new KafkaTemplate<>(transactionalProducerFactory());
    }

    @Bean
    public KafkaTransactionManager<String, OrderMessage> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(transactionalProducerFactory());
    }
}
