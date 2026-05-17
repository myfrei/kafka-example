package com.kafka.demo.case4.service;

import com.kafka.demo.case4.service.model.ActivityEvent;
import com.kafka.demo.case4.service.service.ActivityProcessingService;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * CASE 4 — интеграционный тест на Embedded Kafka.
 *
 * Один инстанс сервиса в тесте получает все 3 партиции. Проверяем, что события
 * с разными ключами разложились по нескольким партициям и все обработаны.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.instance-id=test-instance",
        "app.chaos.failure-rate=0.0"
    }
)
@EmbeddedKafka(partitions = 3, topics = {"partitioned-topic"})
class PartitionConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private ActivityProcessingService processingService;

    @Test
    void distributesKeyedEventsAcrossPartitions() {
        int total = 30;
        try (Producer<String, ActivityEvent> producer = testProducerFactory().createProducer()) {
            for (int i = 0; i < total; i++) {
                String customerId = "customer-" + i;
                ActivityEvent event = new ActivityEvent(
                    customerId, "LOGIN", "EU-WEST", i, Instant.now());
                producer.send(new ProducerRecord<>("partitioned-topic", customerId, event));
            }
            producer.flush();
        }

        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            assertThat(processingService.total()).isEqualTo(total);
            // 30 разных ключей должны разложиться более чем по одной партиции
            assertThat(processingService.countByPartition()).hasSizeGreaterThan(1);
        });
    }

    private DefaultKafkaProducerFactory<String, ActivityEvent> testProducerFactory() {
        Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(broker));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }
}
