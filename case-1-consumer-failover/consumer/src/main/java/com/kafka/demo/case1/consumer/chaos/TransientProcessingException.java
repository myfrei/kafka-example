package com.kafka.demo.case1.consumer.chaos;

/**
 * Временная (transient) ошибка обработки.
 *
 * Имитирует сбой, который в реальной системе мог бы случиться из-за таймаута
 * к внешнему сервису или кратковременной недоступности БД. Такие ошибки имеет
 * смысл повторять (retry), а не сразу отправлять сообщение в DLQ.
 */
public class TransientProcessingException extends RuntimeException {
    public TransientProcessingException(String message) {
        super(message);
    }
}
