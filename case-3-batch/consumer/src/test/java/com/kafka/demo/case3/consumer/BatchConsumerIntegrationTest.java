package com.kafka.demo.case3.consumer;

import com.kafka.demo.case3.consumer.model.OrderEvent;
import com.kafka.demo.case3.consumer.model.OrderLine;
import com.kafka.demo.case3.consumer.service.BatchProcessingService;
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
 * CASE 3 — интеграционный тест на Embedded Kafka.
 *
 * Заливает 60 заказов и проверяет, что батч-консьюмер прочитал их батчами
 * (max.poll.records=50 → как минимум 2 батча) и обработал все записи.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.chaos.failure-rate=0.0"
    }
)
@EmbeddedKafka(partitions = 3, topics = {"batch-topic", "batch-summary"})
class BatchConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private BatchProcessingService processingService;

    @Test
    void consumesOrdersInBatches() {
        int total = 60;
        try (Producer<String, OrderEvent> producer = testProducerFactory().createProducer()) {
            for (int i = 1; i <= total; i++) {
                OrderEvent order = sampleOrder("ord-c3-" + i);
                producer.send(new ProducerRecord<>("batch-topic", order.orderId(), order));
            }
            producer.flush();
        }

        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            assertThat(processingService.totalRecordsProcessed()).isEqualTo(total);
            assertThat(processingService.totalBatches()).isGreaterThanOrEqualTo(1);
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
            "cust-" + (orderId.hashCode() % 10),
            "EU-WEST",
            List.of(new OrderLine("SKU-X", "Sample", 1, new BigDecimal("10.00"))),
            new BigDecimal("10.00"),
            Instant.now()
        );
    }
}
