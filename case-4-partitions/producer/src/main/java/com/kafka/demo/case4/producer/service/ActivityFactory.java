package com.kafka.demo.case4.producer.service;

import com.kafka.demo.case4.producer.model.ActivityEvent;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

/** Генерирует события активности покупателей. */
@Component
public class ActivityFactory {

    private static final String[] TYPES = {"LOGIN", "VIEW", "ADD_TO_CART", "CHECKOUT"};
    private static final String[] REGIONS = {"EU-WEST", "EU-EAST", "US-EAST", "APAC"};

    private final AtomicInteger sequence = new AtomicInteger();

    public ActivityEvent newActivity(String customerId) {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        return new ActivityEvent(
            customerId,
            TYPES[rnd.nextInt(TYPES.length)],
            REGIONS[rnd.nextInt(REGIONS.length)],
            sequence.incrementAndGet(),
            Instant.now()
        );
    }

    public ActivityEvent newActivityForRandomCustomer() {
        return newActivity("customer-" + ThreadLocalRandom.current().nextInt(0, 50));
    }
}
