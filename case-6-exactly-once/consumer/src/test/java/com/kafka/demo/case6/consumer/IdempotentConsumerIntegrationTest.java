package com.kafka.demo.case6.consumer;

import com.kafka.demo.case6.consumer.model.OrderMessage;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * CASE 6 — интеграционный тест на Embedded Kafka + H2.
 *
 * Проверяет exactly-once: консьюмер с read_committed обрабатывает сообщения
 * закоммиченных транзакций и НЕ видит сообщения откатившейся транзакции.
 *
 * Сценарий: коммит заказа A (4 сообщения) → откат заказа B → коммит заказа C
 * (4 сообщения). В processed_messages должно оказаться ровно 8 записей.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.datasource.url=jdbc:h2:mem:case6test;DB_CLOSE_DELAY=-1",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.sql.init.mode=always",
        "app.chaos.failure-rate=0.0"
    }
)
@EmbeddedKafka(
    partitions = 1,
    topics = {"exactly-once-topic"},
    brokerProperties = {
        "transaction.state.log.replication.factor=1",
        "transaction.state.log.min.isr=1",
        "offsets.topic.replication.factor=1"
    }
)
class IdempotentConsumerIntegrationTest {

    private static final String TOPIC = "exactly-once-topic";

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void committedTransactionsConsumedAbortedIgnored() {
        KafkaTemplate<String, OrderMessage> template = transactionalTemplate();

        sendOrder(template, "ord-A", false);  // commit  → 4 сообщения
        sendOrder(template, "ord-B", true);   // rollback → 0 видимых
        sendOrder(template, "ord-C", false);  // commit  → 4 сообщения

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
            assertThat(count("processed_messages")).isEqualTo(8));

        // Откатившийся заказ B не создал записей
        assertThat(count("processed_messages")).isEqualTo(8);
        assertThat(count("orders")).isEqualTo(2);
        assertThat(jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM orders WHERE order_id = 'ord-B'", Integer.class)).isZero();
    }

    private void sendOrder(KafkaTemplate<String, OrderMessage> template, String orderId, boolean abort) {
        try {
            template.executeInTransaction(t -> {
                String cid = UUID.randomUUID().toString();
                t.send(TOPIC, orderId, msg("ORDER_HEADER", orderId, cid, null, 0));
                t.send(TOPIC, orderId, msg("ORDER_ITEM", orderId, cid, "apple", 1));
                t.send(TOPIC, orderId, msg("ORDER_ITEM", orderId, cid, "banana", 2));
                if (abort) {
                    throw new IllegalStateException("simulated rollback");
                }
                t.send(TOPIC, orderId, msg("ORDER_FOOTER", orderId, cid, null, 3));
                return null;
            });
        } catch (IllegalStateException e) {
            if (!abort) {
                throw e;
            }
        }
    }

    private OrderMessage msg(String type, String orderId, String cid, String item, int seq) {
        return new OrderMessage(type, orderId, cid, item, seq, Instant.now());
    }

    private int count(String table) {
        Integer c = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
        return c == null ? 0 : c;
    }

    private KafkaTemplate<String, OrderMessage> transactionalTemplate() {
        Map<String, Object> props = new HashMap<>(KafkaTestUtils.producerProps(broker));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        DefaultKafkaProducerFactory<String, OrderMessage> factory =
            new DefaultKafkaProducerFactory<>(props);
        factory.setTransactionIdPrefix("test-tx-");
        return new KafkaTemplate<>(factory);
    }
}
