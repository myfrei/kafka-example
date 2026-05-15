package com.kafka.demo.case5.consumer.chaos;

/**
 * Временная ошибка — её имеет смысл повторить (retry).
 * Если все повторы исчерпаны — сообщение всё равно уйдёт в DLQ.
 */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }
}
