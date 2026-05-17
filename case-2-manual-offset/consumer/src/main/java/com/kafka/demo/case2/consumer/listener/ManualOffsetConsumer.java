package com.kafka.demo.case2.consumer.listener;

import com.kafka.demo.case2.consumer.chaos.FailureSimulator;
import com.kafka.demo.case2.consumer.model.OrderEvent;
import com.kafka.demo.case2.consumer.repository.KafkaOffsetRepository;
import com.kafka.demo.case2.consumer.service.ManualOffsetService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * CASE 2: Ручное управление оффсетами с хранением в PostgreSQL.
 *
 * Поток обработки:
 * 1. Консьюмер получает типизированный {@link OrderEvent}
 * 2. {@link FailureSimulator} может случайно уронить обработку
 * 3. При успехе — заказ и оффсет пишутся в БД в одной транзакции, затем acknowledge()
 * 4. При сбое — в БД не пишется ничего, оффсет не двигается
 *
 * Replay: при назначении партиций ({@link #onPartitionsAssigned}) консьюмер читает
 * сохранённый оффсет из БД и сдвигает курсор Kafka на него. Поэтому:
 * - упавшее сообщение будет перечитано после рестарта (at-least-once)
 * - можно вручную сдвинуть offset_value в БД и перезапуститься для повтора
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ManualOffsetConsumer implements ConsumerSeekAware {

    private static final String TOPIC = "manual-offset-topic";

    private final KafkaOffsetRepository offsetRepository;
    private final ManualOffsetService manualOffsetService;
    private final FailureSimulator failureSimulator;

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        assignments.forEach((partition, kafkaOffset) -> offsetRepository
            .findByTopicAndPartitionIdAndConsumerGroup(TOPIC, partition.partition(), ManualOffsetService.GROUP)
            .ifPresentOrElse(
                saved -> {
                    long resumeFrom = saved.getOffsetValue() + 1;
                    log.info("Partition {} — resuming from DB offset {} (Kafka was at {})",
                        partition.partition(), resumeFrom, kafkaOffset);
                    callback.seek(partition.topic(), partition.partition(), resumeFrom);
                },
                () -> log.info("Partition {} — no DB offset, using Kafka default (earliest)",
                    partition.partition())
            ));
    }

    @KafkaListener(
        topics = TOPIC,
        groupId = "manual-offset-group",
        containerFactory = "manualAckListenerContainerFactory"
    )
    public void listen(ConsumerRecord<String, OrderEvent> record, Acknowledgment acknowledgment) {
        OrderEvent order = record.value();
        log.info("Received order {} at partition={} offset={}",
            order.orderId(), record.partition(), record.offset());

        try {
            failureSimulator.maybeFail("order " + order.orderId());

            // Бизнес-данные + оффсет пишутся атомарно
            manualOffsetService.persistOrderAndOffset(
                order, record.topic(), record.partition(), record.offset());

            // Только после успешной записи в БД — коммитим оффсет в Kafka
            acknowledgment.acknowledge();
            log.info("Order {} processed; offset {}:{} committed",
                order.orderId(), record.partition(), record.offset());

        } catch (Exception e) {
            log.error("Processing failed for order {} at offset {}:{} — DB offset NOT advanced, "
                    + "message will be re-read on restart/seek: {}",
                order.orderId(), record.partition(), record.offset(), e.getMessage());
            // acknowledge() не вызываем намеренно
        }
    }
}
