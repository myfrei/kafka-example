package com.kafka.demo.case1.consumer;

import com.kafka.demo.case1.consumer.model.OrderEvent;
import com.kafka.demo.case1.consumer.model.OrderLine;
import com.kafka.demo.case1.consumer.service.OrderProcessingService;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * CASE 1 — интеграционный тест на Embedded Kafka.
 *
 * Поднимает встроенный брокер, публикует типизированные {@link OrderEvent},
 * и проверяет, что @KafkaListener десериализовал их и передал в бизнес-логику.
 *
 * chaos.failure-rate=0.0 — сбои отключены, тест детерминирован.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.chaos.failure-rate=0.0",
        "app.consumer-instance-id=test-consumer"
    }
)
@EmbeddedKafka(partitions = 3, topics = {"orders"})
class OrderConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private OrderProcessingService processingService;

    @Test
    void consumesTypedOrderEventsFromTopic() {
        try (Producer<String, OrderEvent> producer = testProducerFactory().createProducer()) {
            for (int i = 1; i <= 3; i++) {
                OrderEvent order = sampleOrder("ord-it-" + i);
                producer.send(new ProducerRecord<>("orders", order.orderId(), order));
            }
            producer.flush();
        }

        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
            assertThat(processingService.wasProcessed("ord-it-1")).isTrue();
            assertThat(processingService.wasProcessed("ord-it-2")).isTrue();
            assertThat(processingService.wasProcessed("ord-it-3")).isTrue();
            assertThat(processingService.distinctOrderCount()).isEqualTo(3);
        });
    }

    private DefaultKafkaProducerFactory<String, OrderEvent> testProducerFactory() {
        Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(broker));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    private OrderEvent sampleOrder(String orderId) {
        return new OrderEvent(
            orderId,
            "cust-1",
            "EU-WEST",
            List.of(new OrderLine("SKU-X", "Sample", 2, new BigDecimal("10.00"))),
            new BigDecimal("20.00"),
            "producer-test",
            Instant.now()
        );
    }
}
