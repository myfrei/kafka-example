package com.kafka.demo.case1.producer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderProducerService {

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Value("${app.kafka.topic}")
    private String topic;

    @Value("${app.producer-id}")
    private String producerId;

    private final AtomicLong counter = new AtomicLong(0);

    /**
     * Автоматически отправляем сообщение каждые 2 секунды.
     * Это позволяет наблюдать непрерывный поток и следить
     * за тем, какой консьюмер получает каждое сообщение.
     */
    @Scheduled(fixedDelay = 2000)
    public void sendAutoMessage() {
        String orderId = UUID.randomUUID().toString().substring(0, 8);
        long seq = counter.incrementAndGet();
        String message = String.format(
            "{\"orderId\":\"%s\", \"producer\":\"%s\", \"seq\":%d, \"time\":\"%s\"}",
            orderId, producerId, seq, LocalDateTime.now()
        );
        sendMessage(orderId, message);
    }

    /**
     * Ручная отправка через REST API.
     * Ключ сообщения (orderId) определяет, в какую партицию попадёт сообщение:
     * partition = hash(key) % numPartitions
     */
    public void sendMessage(String key, String value) {
        CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, value);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[{}] Failed to send message key={}: {}", producerId, key, ex.getMessage());
            } else {
                log.info("[{}] Sent → topic={} partition={} offset={} key={}",
                    producerId,
                    result.getRecordMetadata().topic(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset(),
                    key
                );
            }
        });
    }
}
