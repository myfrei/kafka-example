package com.kafka.demo.case2.consumer;

import com.kafka.demo.case2.consumer.model.OrderEvent;
import com.kafka.demo.case2.consumer.model.OrderLine;
import com.kafka.demo.case2.consumer.repository.KafkaOffsetRepository;
import com.kafka.demo.case2.consumer.repository.ProcessedOrderRepository;
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
 * CASE 2 — интеграционный тест на Embedded Kafka + H2.
 *
 * Проверяет, что консьюмер десериализует {@link OrderEvent}, сохраняет его
 * в БД и атомарно фиксирует Kafka-оффсет.
 *
 * PostgreSQL заменён на H2 in-memory — тест не требует Docker.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:case2test;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "app.chaos.failure-rate=0.0"
    }
)
@EmbeddedKafka(partitions = 3, topics = {"manual-offset-topic"})
class ManualOffsetConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private ProcessedOrderRepository processedOrderRepository;

    @Autowired
    private KafkaOffsetRepository offsetRepository;

    @Test
    void persistsOrderAndOffsetAtomically() {
        try (Producer<String, OrderEvent> producer = testProducerFactory().createProducer()) {
            for (int i = 1; i <= 5; i++) {
                OrderEvent order = sampleOrder("ord-c2-" + i);
                producer.send(new ProducerRecord<>("manual-offset-topic", order.orderId(), order));
            }
            producer.flush();
        }

        await().atMost(Duration.ofSeconds(25)).untilAsserted(() -> {
            assertThat(processedOrderRepository.count()).isEqualTo(5);
            // Оффсет сохранён хотя бы для одной партиции
            assertThat(offsetRepository.count()).isGreaterThan(0);
        });

        assertThat(processedOrderRepository.findByOrderId("ord-c2-3")).isPresent();
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
            List.of(new OrderLine("SKU-X", "Sample", 1, new BigDecimal("99.00"))),
            new BigDecimal("99.00"),
            Instant.now()
        );
    }
}
