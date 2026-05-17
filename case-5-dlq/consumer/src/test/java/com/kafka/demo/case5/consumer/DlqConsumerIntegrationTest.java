package com.kafka.demo.case5.consumer;

import com.kafka.demo.case5.consumer.model.OrderEvent;
import com.kafka.demo.case5.consumer.service.OrderProcessingService;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * CASE 5 — интеграционный тест на Embedded Kafka.
 *
 * Проверяет, что нормальные заказы обрабатываются, а «ядовитый» заказ
 * (customerId "bad-*") после неудачной обработки уходит в DLQ-топик
 * orders-dlq-demo.DLT через DeadLetterPublishingRecoverer.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.chaos.failure-rate=0.0"
    }
)
@EmbeddedKafka(partitions = 3, topics = {"orders-dlq-demo", "orders-dlq-demo.DLT"})
class DlqConsumerIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private OrderProcessingService processingService;

    @Test
    void goodOrdersProcessedAndPoisonOrderGoesToDlq() {
        try (Producer<String, OrderEvent> producer = testProducerFactory().createProducer()) {
            for (int i = 1; i <= 3; i++) {
                producer.send(new ProducerRecord<>("orders-dlq-demo", "ord-good-" + i,
                    order("ord-good-" + i, "cust-" + i)));
            }
            producer.send(new ProducerRecord<>("orders-dlq-demo", "ord-poison",
                order("ord-poison", "bad-customer")));
            producer.flush();
        }

        await().atMost(Duration.ofSeconds(25))
            .untilAsserted(() -> assertThat(processingService.processedCount()).isEqualTo(3));

        // «Ядовитый» заказ должен оказаться в DLQ-топике
        try (Consumer<String, String> dlqConsumer = testDlqConsumer()) {
            dlqConsumer.subscribe(java.util.List.of("orders-dlq-demo.DLT"));
            ConsumerRecords<String, String> dlqRecords =
                KafkaTestUtils.getRecords(dlqConsumer, Duration.ofSeconds(15), 1);
            assertThat(dlqRecords.count()).isEqualTo(1);
            ConsumerRecord<String, String> dlqRecord = dlqRecords.iterator().next();
            assertThat(dlqRecord.key()).isEqualTo("ord-poison");
            assertThat(dlqRecord.value()).contains("bad-customer");
        }
    }

    private OrderEvent order(String orderId, String customerId) {
        return new OrderEvent(orderId, customerId, new BigDecimal("100.00"), "test", Instant.now());
    }

    private DefaultKafkaProducerFactory<String, OrderEvent> testProducerFactory() {
        Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(broker));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    private Consumer<String, String> testDlqConsumer() {
        Map<String, Object> props = new HashMap<>(
            KafkaTestUtils.consumerProps("dlq-test-verifier", "true", broker));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new DefaultKafkaConsumerFactory<String, String>(props).createConsumer();
    }
}
