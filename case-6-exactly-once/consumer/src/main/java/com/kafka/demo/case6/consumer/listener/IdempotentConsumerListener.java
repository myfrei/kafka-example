package com.kafka.demo.case6.consumer.listener;

import com.kafka.demo.case6.consumer.model.OrderMessage;
import com.kafka.demo.case6.consumer.service.IdempotentOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * CASE 6: Idempotent Consumer.
 *
 * isolation.level=read_committed (см. IsolatedConsumerConfig) гарантирует, что
 * сюда не попадут сообщения откатившихся транзакций. Дедупликация по messageId
 * в {@link IdempotentOrderService} защищает от повторной доставки.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class IdempotentConsumerListener {

    private final IdempotentOrderService orderService;

    @KafkaListener(
        topics = "exactly-once-topic",
        groupId = "exactly-once-group",
        containerFactory = "isolatedListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, OrderMessage> record, Acknowledgment acknowledgment) {
        String messageId = record.topic() + ":" + record.partition() + ":" + record.offset();

        // Исключение пробрасываем — транзакция БД откатится, оффсет не закоммитится
        orderService.processIfNew(messageId, record.value(),
            record.topic(), record.partition(), record.offset());

        acknowledgment.acknowledge();
    }
}
