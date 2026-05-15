package com.kafka.demo.case3.consumer.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CASE 3: Batch Consumer — чтение сообщений батчами с ручным коммитом оффсета
 *
 * Ключевые концепции:
 *
 * 1. batchListener=true — listener получает List<ConsumerRecord> вместо одной записи
 * 2. max.poll.records=50 — максимум 50 сообщений за один poll()
 * 3. Ручной коммит ПОСЛЕ обработки всего батча
 *
 * Преимущества батч-обработки:
 * - Меньше overhead на network round-trips (один коммит на весь батч)
 * - Возможность bulk-insert в БД (значительно быстрее построчной вставки)
 * - Агрегация/дедупликация сообщений перед обработкой
 *
 * Риски:
 * - Если упало на середине батча — все сообщения батча перечитаются (at-least-once)
 * - Нужна идемпотентная обработка
 *
 * Стратегии движения оффсета в батче:
 * 1. Коммит всего батча после успешной обработки (этот пример)
 * 2. Коммит по каждой записи (нивелирует преимущества батча)
 * 3. Коммит с партиционированием — по максимальному оффсету каждой партиции
 */
@Slf4j
@Component
public class BatchConsumerListener {

    @KafkaListener(
        topics = "batch-topic",
        groupId = "batch-consumer-group",
        containerFactory = "batchListenerContainerFactory"
    )
    public void listenBatch(
        List<ConsumerRecord<String, String>> records,
        Acknowledgment acknowledgment
    ) {
        if (records.isEmpty()) {
            return;
        }

        log.info("=== BATCH RECEIVED: {} messages ===", records.size());
        logBatchStats(records);

        try {
            // Обработка всего батча
            processBatch(records);

            // Коммитим оффсет ТОЛЬКО после успешной обработки ВСЕГО батча
            // Kafka сама возьмёт максимальный оффсет по каждой партиции
            acknowledgment.acknowledge();

            log.info("=== BATCH COMMITTED: {} messages processed ===", records.size());

        } catch (Exception e) {
            log.error("Batch processing failed at batch of {} records. NOT committing offset. Error: {}",
                records.size(), e.getMessage());
            // Не коммитим — весь батч будет перечитан при следующем poll()
            // В реальных системах здесь обычно реализуют retry с backoff
        }
    }

    /**
     * Демонстрирует типичную батч-операцию:
     * вместо N отдельных INSERT'ов — один bulk INSERT
     */
    private void processBatch(List<ConsumerRecord<String, String>> records) {
        // Группируем по ключу для дедупликации
        Map<String, ConsumerRecord<String, String>> deduped = records.stream()
            .collect(Collectors.toMap(
                ConsumerRecord::key,
                r -> r,
                (existing, newOne) -> newOne   // Берём последнюю версию при дублях
            ));

        log.info("After deduplication: {} unique keys (from {} records)",
            deduped.size(), records.size());

        // Симулируем bulk insert (в реальности — jdbcTemplate.batchUpdate() или JPA saveAll())
        deduped.values().forEach(record ->
            log.debug("  Bulk-insert: partition={} offset={} key={} value={}",
                record.partition(), record.offset(), record.key(), record.value())
        );

        // Симулируем время обработки батча
        simulateProcessing(records.size());
    }

    /**
     * Логирует статистику батча — распределение по партициям и диапазоны оффсетов.
     * Полезно для отладки и мониторинга.
     */
    private void logBatchStats(List<ConsumerRecord<String, String>> records) {
        Map<Integer, LongSummaryStatistics> statsByPartition = records.stream()
            .collect(Collectors.groupingBy(
                ConsumerRecord::partition,
                Collectors.summarizingLong(ConsumerRecord::offset)
            ));

        statsByPartition.forEach((partition, stats) ->
            log.info("  Partition {}: {} records, offsets [{} - {}]",
                partition, stats.getCount(), stats.getMin(), stats.getMax())
        );
    }

    private void simulateProcessing(int batchSize) {
        try {
            // 10ms на каждое сообщение в батче
            Thread.sleep(batchSize * 10L);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
