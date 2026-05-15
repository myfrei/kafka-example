package com.kafka.demo.case4.service.model;

import java.time.Instant;

/** Событие активности покупателя, прочитанное из partitioned-topic. */
public record ActivityEvent(
    String customerId,
    String activityType,
    String region,
    int sequenceNo,
    Instant timestamp
) {
}
