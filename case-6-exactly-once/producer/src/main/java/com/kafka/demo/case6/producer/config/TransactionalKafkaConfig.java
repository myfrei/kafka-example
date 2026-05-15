package com.kafka.demo.case6.producer.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;

import java.util.HashMap;
import java.util.Map;

/**
 * Конфигурация транзакционного (идемпотентного) продюсера.
 *
 * Разница с обычным продюсером:
 * 1. enable.idempotence=true — дедупликация на уровне broker
 * 2. transactional.id — уникальный ID для транзакционного продюсера
 * 3. acks=all — обязательно для идемпотентного продюсера
 * 4. max.in.flight.requests.per.connection=5 — максимум для идемпотентности
 */
@Configuration
public class TransactionalKafkaConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, String> transactionalProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        // --- Idempotence settings ---
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        // --- Transaction settings ---
        // transactional.id должен быть уникален для каждого инстанса продюсера
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "case6-producer-tx");

        var factory = new DefaultKafkaProducerFactory<String, String>(props);
        // Указываем transaction id prefix для Spring Kafka
        factory.setTransactionIdPrefix("case6-tx-");
        return factory;
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(transactionalProducerFactory());
    }

    @Bean
    public KafkaTransactionManager<String, String> kafkaTransactionManager() {
        return new KafkaTransactionManager<>(transactionalProducerFactory());
    }
}
