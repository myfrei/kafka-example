# Case 5 — Dead Letter Queue (DLQ)

## Описание кейса

Демонстрирует паттерн Dead Letter Queue для обработки сообщений, которые не удалось обработать.

**Проблема без DLQ:**
Если обработка сообщения упала → консьюмер застрял на этом оффсете → все последующие сообщения не читаются → блокировка всего топика.

**Решение с DLQ:**
```
[main-topic] → Consumer → (ошибка? retry 3x) ──fail──→ [main-topic.DLQ]
                              ↓ success                        ↓
                           next message               DLQ Handler (алерт/анализ)
```

**Стратегии работы с DLQ:**
1. **Ignore** — просто логировать и продолжать (потеря данных!)
2. **Retry** — повторять N раз, потом в DLQ (используем здесь)
3. **Circuit Breaker** — при превышении % ошибок прекратить обработку
4. **Manual Review** — UI для операторов, ручное переигрывание

---

## Запуск

```bash
docker-compose up -d zookeeper kafka kafka-ui
docker-compose --profile case5 up -d

docker logs -f case5-consumer &
docker logs -f case5-dlq-handler &
```

---

## Сценарий проверки

### Шаг 1: Успешное сообщение

```bash
curl -X POST http://localhost:8088/api/orders \
  -H "Content-Type: application/json" \
  -d '{"key": "order-ok", "message": "{\"orderId\": \"ok-1\", \"product\": \"apple\"}"}'
```

Лог consumer: `Successfully processed: key=order-ok`

### Шаг 2: Сообщение с постоянной ошибкой (уйдёт в DLQ)

```bash
curl -X POST http://localhost:8088/api/orders \
  -H "Content-Type: application/json" \
  -d '{"key": "fail-order", "message": "{\"orderId\": \"bad-1\", \"status\": \"fail\"}"}'
```

Логи consumer:
```
Processing failed (attempt 1/3): key=fail-order error=Simulated permanent failure
Processing failed (attempt 2/3): key=fail-order error=Simulated permanent failure
Processing failed (attempt 3/3): key=fail-order error=Simulated permanent failure
Message sent to DLQ: key=fail-order topic=orders.DLQ
```

Лог dlq-handler:
```
╔═══════════════════════════════════════════╗
║  DLQ MESSAGE RECEIVED — REQUIRES ATTENTION
╠═══════════════════════════════════════════╣
║  Key:       fail-order
║  Value:     {"original": {...}, "error": "Simulated permanent failure", ...}
╚═══════════════════════════════════════════╝
```

### Шаг 3: Проверить Kafka UI

- **orders-dlq-demo** — основной топик (все сообщения)
- **orders.DLQ** — только упавшие сообщения
- Consumer Groups → dlq-consumer-group → lag = 0 (не застрял!)

### Шаг 4: Replay из DLQ

После исправления кода — перечитать сообщения из DLQ:

```bash
# Переименовать DLQ-топик в основной (или написать отдельный сервис replay)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic orders.DLQ \
  --from-beginning
```

---

## Spring Kafka встроенная поддержка DLQ

Spring Kafka предоставляет `DeadLetterPublishingRecoverer` — настраивается в конфигурации:

```java
@Bean
public DefaultErrorHandler errorHandler(KafkaTemplate<?, ?> template) {
    // Автоматически отправляет в topic.DLQ после N попыток
    var recoverer = new DeadLetterPublishingRecoverer(template);
    var backoff = new ExponentialBackOffWithMaxRetries(3);
    backoff.setInitialInterval(1000);
    backoff.setMultiplier(2.0);
    return new DefaultErrorHandler(recoverer, backoff);
}
```

Это встроенное решение автоматически:
- Добавляет заголовки с исходным топиком, партицией, оффсетом
- Следует стратегии именования `{topic}.DLQ`
- Реализует exponential backoff
