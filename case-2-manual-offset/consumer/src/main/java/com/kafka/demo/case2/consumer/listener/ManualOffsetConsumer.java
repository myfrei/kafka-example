package com.kafka.demo.case2.consumer.listener;

import com.kafka.demo.case2.consumer.repository.KafkaOffsetRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.Optional;

/**
 * CASE 2: Ручное управление оффсетами с хранением в PostgreSQL
 *
 * Ключевые концепции:
 *
 * 1. enable-auto-commit: false — отключаем автоматический коммит
 * 2. AckMode.MANUAL — коммитим оффсет только после успешной обработки
 * 3. ConsumerSeekAware — при старте читаем оффсет из БД и сдвигаем Kafka-курсор
 *
 * Сценарий:
 * - Консьюмер получает сообщение
 * - Обрабатывает (бизнес-логика)
 * - Сохраняет оффсет в PostgreSQL в той же транзакции
 * - Только потом коммитит оффсет в Kafka
 *
 * Преимущество: если сохранение в БД упадёт — оффсет не сдвинется,
 * и сообщение будет перечитано (at-least-once гарантия).
 *
 * Ручной сдвиг оффсета (replay):
 * - Можно вручную поменять offset_value в БД и перезапустить сервис
 * - Консьюмер перечитает сообщения с нужного места
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ManualOffsetConsumer implements ConsumerSeekAware {

    private static final String TOPIC = "manual-offset-topic";
    private static final String GROUP = "manual-offset-group";

    private final KafkaOffsetRepository offsetRepository;
    private ConsumerSeekCallback seekCallback;

    /**
     * При инициализации ConsumerSeekAware Kafka передаёт callback для seek.
     * Сохраняем его для последующего использования при onPartitionsAssigned.
     */
    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        this.seekCallback = callback;
    }

    /**
     * При назначении партиций — читаем сохранённый оффсет из БД
     * и сдвигаем Kafka-курсор на это место.
     *
     * Это позволяет возобновить чтение с точно того места, где остановились,
     * даже если __consumer_offsets был сброшен или сервис упал.
     */
    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.forEach((partition, currentOffset) -> {
            Optional<Long> savedOffset = offsetRepository
                .findByTopicAndPartitionIdAndConsumerGroup(TOPIC, partition.partition(), GROUP)
                .map(entity -> entity.getOffsetValue());

            if (savedOffset.isPresent()) {
                long resumeFrom = savedOffset.get();
                log.info("Partition {} — resuming from DB offset: {} (Kafka current: {})",
                    partition.partition(), resumeFrom, currentOffset);
                // Сдвигаем курсор на следующее после сохранённого сообщение
                callback.seek(partition.topic(), partition.partition(), resumeFrom + 1);
            } else {
                log.info("Partition {} — no DB offset found, using Kafka default (earliest)", partition.partition());
            }
        });
    }

    /**
     * AckMode.MANUAL_IMMEDIATE: acknowledgment.acknowledge() немедленно коммитит оффсет в Kafka.
     * Мы же сначала сохраняем в БД, потом коммитим — это даёт at-least-once семантику.
     */
    @KafkaListener(
        topics = TOPIC,
        groupId = GROUP,
        containerFactory = "manualAckListenerContainerFactory"
    )
    @Transactional
    public void listen(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        log.info("Received: partition={} offset={} key={} value={}",
            record.partition(), record.offset(), record.key(), record.value());

        try {
            // 1. Бизнес-логика
            processMessage(record.value());

            // 2. Сохраняем оффсет в PostgreSQL (в той же транзакции)
            offsetRepository.upsertOffset(
                record.topic(),
                record.partition(),
                record.offset(),
                GROUP
            );

            // 3. Только после успешного сохранения в БД — коммитим в Kafka
            acknowledgment.acknowledge();

            log.info("Successfully processed and committed offset: partition={} offset={}",
                record.partition(), record.offset());

        } catch (Exception e) {
            log.error("Failed to process message at partition={} offset={}. NOT committing offset.",
                record.partition(), record.offset(), e);
            // Не вызываем acknowledge() — при следующем poll() сообщение будет перечитано
        }
    }

    private void processMessage(String value) {
        // Симулируем бизнес-логику
        log.debug("Processing: {}", value);
        // Если нужно симулировать ошибку — бросаем исключение здесь
    }
}
