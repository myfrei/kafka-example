package com.kafka.demo.case5.dlq.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * DLQ Handler — обработчик Dead Letter Queue
 *
 * Читает сообщения из топика orders.DLQ и:
 * 1. Логирует для алертинга (в реальности → Slack/PagerDuty)
 * 2. Сохраняет в БД для ручного анализа
 * 3. Предоставляет REST API для просмотра и повторной обработки
 *
 * В реальных системах DLQ Handler может:
 * - Автоматически повторять обработку после исправления кода
 * - Отправлять алерты команде
 * - Предоставлять UI для операторов
 * - Группировать ошибки по типам
 */
@Slf4j
@Component
public class DlqHandlerListener {

    // Хранилище DLQ сообщений (в реальности — PostgreSQL)
    private final List<DlqMessage> dlqMessages = new CopyOnWriteArrayList<>();

    @KafkaListener(
        topics = "orders.DLQ",
        groupId = "dlq-handler-group"
    )
    public void handleDlqMessage(ConsumerRecord<String, String> record) {
        DlqMessage dlqMessage = new DlqMessage(
            record.key(),
            record.value(),
            record.partition(),
            record.offset()
        );
        dlqMessages.add(dlqMessage);

        // В реальной системе здесь:
        // - alertService.sendSlackAlert(dlqMessage);
        // - dlqRepository.save(dlqMessage);
        // - metricsService.incrementDlqCounter(errorType);

        log.error("""
            ╔═══════════════════════════════════════════╗
            ║  DLQ MESSAGE RECEIVED — REQUIRES ATTENTION
            ╠═══════════════════════════════════════════╣
            ║  Key:       {}
            ║  Partition: {}
            ║  Offset:    {}
            ║  Value:     {}
            ╚═══════════════════════════════════════════╝
            """,
            record.key(), record.partition(), record.offset(), record.value()
        );
    }

    public List<DlqMessage> getDlqMessages() {
        return new ArrayList<>(dlqMessages);
    }

    public record DlqMessage(
        String key,
        String value,
        int partition,
        long offset
    ) {}
}
