package com.kafka.demo.case5.consumer.listener;

import com.kafka.demo.case5.consumer.model.OrderEvent;
import com.kafka.demo.case5.consumer.service.OrderProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * CASE 5: Dead Letter Queue (DLQ).
 *
 * Listener просто вызывает бизнес-логику. Всю работу с ошибками выполняет
 * {@link com.kafka.demo.case5.consumer.config.DlqErrorHandlingConfig}:
 * retry + публикация в DLQ-топик.
 *
 * Топология:
 * [orders-dlq-demo] → Consumer → (ошибка? retry) → [orders-dlq-demo.DLT]
 *                                                          ↓
 *                                          dlq-handler + alerting-service
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DlqConsumerListener {

    private final OrderProcessingService processingService;

    @KafkaListener(topics = "orders-dlq-demo", groupId = "dlq-consumer-group")
    public void listen(ConsumerRecord<String, OrderEvent> record) {
        log.info("Processing order {} at partition={} offset={}",
            record.key(), record.partition(), record.offset());
        // Исключения намеренно пробрасываем — их обработает DefaultErrorHandler
        processingService.process(record.value());
    }
}
