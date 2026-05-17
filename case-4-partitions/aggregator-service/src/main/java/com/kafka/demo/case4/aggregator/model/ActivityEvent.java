package com.kafka.demo.case4.aggregator.model;

import java.time.Instant;

public record ActivityEvent(
    String customerId,
    String activityType,
    String region,
    int sequenceNo,
    Instant timestamp
) {
}
