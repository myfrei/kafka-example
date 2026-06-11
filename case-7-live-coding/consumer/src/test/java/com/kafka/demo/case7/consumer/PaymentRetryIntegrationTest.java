package com.kafka.demo.case7.consumer;

import com.kafka.demo.case7.consumer.model.PaymentEvent;
import com.kafka.demo.case7.consumer.service.PaymentProcessingService;
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
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * CASE 7 — интеграционный тест на Embedded Kafka (Docker не нужен).
 *
 * Цель: нормальный платёж обрабатывается, а poison (customerId "bad-*") уходит в DLT
 * БЕЗ повторов. Реализуйте тело теста — TODO (6). app.chaos.failure-rate=0.0 (детерминизм).
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "app.chaos.failure-rate=0.0"
    }
)
@EmbeddedKafka(partitions = 3, topics = {"payments-retry-demo"})
class PaymentRetryIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private PaymentProcessingService processingService;

    @Test
    void poisonGoesToDltAndNormalProcessed() {
        // =================================================================
        // TODO (6): реализовать сценарий:
        //   1) Через testProducerFactory() отправить в "payments-retry-demo":
        //        - нормальный платёж: payment("pay-1", "cust-1")
        //        - poison-платёж:     payment("pay-2", "bad-customer")
        //      (producer.send(new ProducerRecord<>(...)); затем producer.flush();)
        //   2) Дождаться результата через Awaitility, например:
        //        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> {
        //            assertThat(processingService.processedCount()).isEqualTo(1);
        //            assertThat(processingService.dltCount()).isEqualTo(1);
        //        });
        //   Нужные статические импорты:
        //     static org.assertj.core.api.Assertions.assertThat;
        //     static org.awaitility.Awaitility.await;   // + java.time.Duration
        // =================================================================
        org.junit.jupiter.api.Assertions.fail("TODO (6): реализовать тест poison→DLT и happy-path");
    }

    /** [ГОТОВО] фабрика тест-платежей */
    private PaymentEvent payment(String paymentId, String customerId) {
        return new PaymentEvent(paymentId, "ord-1", customerId, new BigDecimal("100.00"), Instant.now());
    }

    /** [ГОТОВО] продюсер с JSON-сериализацией (как в продакшен-настройках) */
    private Producer<String, PaymentEvent> testProducerFactory() {
        Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(broker));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<String, PaymentEvent>(props).createProducer();
    }
}
