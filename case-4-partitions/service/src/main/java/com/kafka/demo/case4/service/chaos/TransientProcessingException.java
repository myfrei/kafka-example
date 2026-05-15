package com.kafka.demo.case4.service.chaos;

/** Временная ошибка обработки события активности. */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }
}
