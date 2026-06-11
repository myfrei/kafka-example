package com.kafka.demo.case7.consumer.chaos;

/**
 * Временная ошибка — её имеет смысл повторить (retry-топики).
 * Если все повторы исчерпаны — сообщение уйдёт в DLT. [ГОТОВО]
 */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }
}
