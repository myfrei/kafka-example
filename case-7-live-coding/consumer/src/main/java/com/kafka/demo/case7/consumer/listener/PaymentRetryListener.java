package com.kafka.demo.case7.consumer.listener;

import com.kafka.demo.case7.consumer.model.PaymentEvent;
import com.kafka.demo.case7.consumer.service.PaymentProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

/**
 * CASE 7 — НЕблокирующие повторы через retry-топики (ГЛАВНАЯ ЧАСТЬ ЗАДАЧИ).
 *
 * В отличие от Case 5 (DefaultErrorHandler — блокирующие ретраи, партиция «стоит»
 * на время backoff), здесь Spring Kafka перекладывает упавшее сообщение в отдельный
 * retry-топик с задержкой, а основной поток продолжает читать следующие сообщения.
 *
 * Что должно получиться после реализации:
 *   payments-retry-demo
 *     ├─ success ─────────────► PaymentProcessingService.process()
 *     ├─ transient fail ─► payments-retry-demo-retry-0 ─► -retry-1 ─► -retry-2 ─► (успех / DLT)
 *     └─ poison (bad-*) ─────────────────────────────► payments-retry-demo-dlt (без повторов)
 *
 * Нужные импорты (добавьте по мере реализации):
 *   org.springframework.kafka.annotation.KafkaListener
 *   org.springframework.kafka.annotation.RetryableTopic
 *   org.springframework.kafka.annotation.DltHandler
 *   org.springframework.kafka.retrytopic.DltStrategy
 *   org.springframework.kafka.retrytopic.TopicSuffixingStrategy
 *   org.springframework.kafka.support.KafkaHeaders
 *   org.springframework.messaging.handler.annotation.Header
 *   org.springframework.retry.annotation.Backoff
 *   com.kafka.demo.case7.consumer.exception.PoisonMessageException
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PaymentRetryListener {

    private final PaymentProcessingService processingService;

    // =====================================================================
    // TODO (4) — основной слушатель с НЕблокирующими повторами:
    //
    //   a) Пометьте метод @KafkaListener(topics = "payments-retry-demo",
    //                                     groupId = "payment-retry-group")
    //
    //   b) Добавьте @RetryableTopic со свойствами:
    //        attempts = "4"                       // 1 исходная попытка + 3 повтора
    //        backoff = @Backoff(delay = 2000, multiplier = 2.0, maxDelay = 16000)
    //        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE
    //                                             // предсказуемые имена -retry-0/1/2
    //        dltStrategy = DltStrategy.FAIL_ON_ERROR
    //        exclude = { PoisonMessageException.class }   // poison сразу в DLT, без повторов
    //        autoCreateTopics = "true"
    //        numPartitions = "3"
    //
    //   c) В теле — просто вызвать processingService.process(record.value()).
    //      Исключения НЕ ловить: их перехватит инфраструктура retry-топиков.
    //
    //   ВАЖНО: чтобы это заработало, на Case7ConsumerApplication должен стоять
    //          @EnableKafkaRetryTopic (см. TODO 4a).
    // =====================================================================
    public void onPayment(ConsumerRecord<String, PaymentEvent> record) {
        throw new UnsupportedOperationException(
            "TODO (4): реализовать @KafkaListener + @RetryableTopic, вызвать processingService.process(...)");
    }

    // =====================================================================
    // TODO (5) — финальный обработчик DLT:
    //
    //   a) Пометьте метод аннотацией @DltHandler
    //
    //   b) Достаньте метаданные сбоя из ЗАГОЛОВКОВ (через параметры с @Header):
    //        @Header(KafkaHeaders.ORIGINAL_TOPIC)     String originalTopic
    //        @Header(KafkaHeaders.EXCEPTION_FQCN)      String exceptionFqcn
    //        @Header(KafkaHeaders.EXCEPTION_MESSAGE)   String exceptionMessage
    //      ВНИМАНИЕ: для retry-топиков это ORIGINAL_*/EXCEPTION_*, а НЕ DLT_*
    //      (DLT_* ставит standalone DeadLetterPublishingRecoverer из Case 5).
    //
    //   c) Залогируйте сбой и вызовите processingService.registerDlt().
    // =====================================================================
    public void onDlt(ConsumerRecord<String, PaymentEvent> record) {
        throw new UnsupportedOperationException(
            "TODO (5): пометить @DltHandler, прочитать заголовки ORIGINAL_*/EXCEPTION_*, вызвать registerDlt()");
    }
}
