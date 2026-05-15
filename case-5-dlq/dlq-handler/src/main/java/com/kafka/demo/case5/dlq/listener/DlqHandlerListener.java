package com.kafka.demo.case5.dlq.listener;

import com.kafka.demo.case5.dlq.model.DlqRecord;
import com.kafka.demo.case5.dlq.model.OrderEvent;
import com.kafka.demo.case5.dlq.service.DlqService;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * DLQ Handler — читает топик orders-dlq-demo.DLT.
 *
 * DeadLetterPublishingRecoverer кладёт в заголовки сообщения метаданные сбоя:
 * исходный топик/партицию/оффсет и текст исключения. Этот сервис достаёт их
 * из заголовков и сохраняет полную запись о проблеме.
 */
@Component
@RequiredArgsConstructor
public class DlqHandlerListener {

    private final DlqService dlqService;

    @KafkaListener(topics = "orders-dlq-demo.DLT", groupId = "dlq-handler-group")
    public void handle(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent order = record.value();
        dlqService.store(new DlqRecord(
            order != null ? order.orderId() : record.key(),
            order != null ? order.customerId() : null,
            headerAsString(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
            headerAsInt(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
            headerAsLong(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
            headerAsString(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
            headerAsString(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE),
            Instant.now()
        ));
    }

    private String headerAsString(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
    }

    private Integer headerAsInt(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : ByteBuffer.wrap(h.value()).getInt();
    }

    private Long headerAsLong(ConsumerRecord<?, ?> record, String name) {
        Header h = record.headers().lastHeader(name);
        return h == null ? null : ByteBuffer.wrap(h.value()).getLong();
    }
}
