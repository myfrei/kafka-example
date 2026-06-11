package com.kafka.demo.case7.consumer.exception;

/**
 * «Ядовитое» сообщение — обработка падает детерминированно и навсегда.
 * Повторять бессмысленно: должно быть non-retryable и уходить в DLT сразу. [ГОТОВО]
 */
public class PoisonMessageException extends RuntimeException {
    public PoisonMessageException(String message) {
        super(message);
    }
}
