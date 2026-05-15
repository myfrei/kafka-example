package com.kafka.demo.case2.consumer.chaos;

/** Временная ошибка обработки — заказ не записан, оффсет в БД не сдвинут. */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }
}
