package com.kafka.demo.case3.consumer.chaos;

/** Временная ошибка обработки батча. */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }
}
