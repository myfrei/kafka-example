package com.kafka.demo.case7.producer.service;

import com.kafka.demo.case7.producer.model.PaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Публикует платежи в топик payments-retry-demo.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentProducerService {

    public static final String TOPIC = "payments-retry-demo";

    private final KafkaTemplate<String, PaymentEvent> kafkaTemplate;
    private final PaymentFactory factory;

    /** [ГОТОВО] обычный платёж */
    public PaymentEvent publishRandom() {
        return publish(factory.newPayment());
    }

    /** [ГОТОВО] «ядовитый» платёж — должен уйти в DLT */
    public PaymentEvent publishPoison() {
        return publish(factory.newPoisonPayment());
    }

    // =====================================================================
    // TODO (1): отправить платёж в топик TOPIC, используя customerId как КЛЮЧ.
    //   Зачем ключ: murmur2(key) % partitions → один покупатель всегда в одну
    //   партицию (порядок его платежей сохраняется).
    //   Подсказка: kafkaTemplate.send(TOPIC, payment.customerId(), payment);
    //   Не забудьте вернуть payment и залогировать отправку.
    // =====================================================================
    public PaymentEvent publish(PaymentEvent payment) {
        throw new UnsupportedOperationException(
            "TODO (1): отправить payment в Kafka с ключом customerId и вернуть payment");
    }
}
