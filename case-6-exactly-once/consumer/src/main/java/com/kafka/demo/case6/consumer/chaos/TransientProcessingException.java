package com.kafka.demo.case6.consumer.chaos;

/** Временная ошибка обработки — транзакция БД откатывается, оффсет не коммитится. */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }
}
