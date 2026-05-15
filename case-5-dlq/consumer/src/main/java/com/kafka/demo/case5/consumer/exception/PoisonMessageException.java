package com.kafka.demo.case5.consumer.exception;

/**
 * «Ядовитое» сообщение — обработка падает детерминированно и навсегда.
 *
 * Повторять такое сообщение бессмысленно, поэтому оно помечено как
 * non-retryable и сразу отправляется в DLQ.
 */
public class PoisonMessageException extends RuntimeException {
    public PoisonMessageException(String message) {
        super(message);
    }
}
