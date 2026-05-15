package com.kafka.demo.case3.consumer.listener;

import com.kafka.demo.case3.consumer.chaos.FailureSimulator;
import com.kafka.demo.case3.consumer.model.BatchSummary;
import com.kafka.demo.case3.consumer.model.OrderEvent;
import com.kafka.demo.case3.consumer.service.BatchProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CASE 3: Batch Consumer — чтение сообщений батчами с ручным коммитом оффсета.
 *
 * Ключевые концепции:
 * 1. batchListener=true — listener получает List<ConsumerRecord> вместо одной записи
 * 2. max.poll.records=50 — максимум 50 сообщений за один poll()
 * 3. Ручной коммит ПОСЛЕ обработки всего батча
 *
 * Что добавлено в реалистичной версии:
 * - В батч приходит типизированный {@link OrderEvent}
 * - После успешного коммита публикуется {@link BatchSummary} в топик batch-summary
 *   (его агрегирует reporting-service)
 * - {@link FailureSimulator} может уронить ВЕСЬ батч — он не коммитится целиком
 *
 * Риск батч-обработки: при сбое в середине весь батч перечитается (at-least-once),
 * поэтому обработка обязана быть идемпотентной.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class BatchConsumerListener {

    public static final String SUMMARY_TOPIC = "batch-summary";

    private final BatchProcessingService processingService;
    private final FailureSimulator failureSimulator;
    private final KafkaTemplate<String, BatchSummary> kafkaTemplate;

    @KafkaListener(
        topics = "batch-topic",
        groupId = "batch-consumer-group",
        containerFactory = "batchListenerContainerFactory"
    )
    public void listenBatch(List<ConsumerRecord<String, OrderEvent>> records, Acknowledgment acknowledgment) {
        if (records.isEmpty()) {
            return;
        }

        log.info("=== BATCH RECEIVED: {} records ===", records.size());
        logBatchStats(records);

        try {
            // Сбой имитируется на уровне всего батча
            failureSimulator.maybeFailBatch(records.size());

            List<OrderEvent> orders = records.stream()
                .map(ConsumerRecord::value)
                .toList();
            BatchSummary summary = processingService.processBatch(orders);

            // Коммитим оффсет ТОЛЬКО после успешной обработки всего батча
            acknowledgment.acknowledge();

            // Публикуем сводку для reporting-service
            kafkaTemplate.send(SUMMARY_TOPIC, summary.batchId(), summary);

            log.info("=== BATCH COMMITTED: {} records, summary {} published ===",
                records.size(), summary.batchId());

        } catch (Exception e) {
            log.error("Batch processing failed ({} records). NOT committing offset — "
                + "batch will be re-read on next poll/restart: {}", records.size(), e.getMessage());
        }
    }

    private void logBatchStats(List<ConsumerRecord<String, OrderEvent>> records) {
        Map<Integer, LongSummaryStatistics> byPartition = records.stream()
            .collect(Collectors.groupingBy(
                ConsumerRecord::partition,
                Collectors.summarizingLong(ConsumerRecord::offset)
            ));
        byPartition.forEach((partition, stats) ->
            log.info("  Partition {}: {} records, offsets [{} - {}]",
                partition, stats.getCount(), stats.getMin(), stats.getMax()));
    }
}
