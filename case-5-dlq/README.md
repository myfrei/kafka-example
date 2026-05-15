# Case 5 — Dead Letter Queue (DLQ)

## Описание кейса

Демонстрирует паттерн Dead Letter Queue для обработки сообщений, которые не удалось обработать.

**Проблема без DLQ:**
Если обработка сообщения упала и его повторять бесконечно — консьюмер застрянет
на этом оффсете, и весь топик встанет.

**Решение с DLQ (через стандартные средства Spring Kafka):**
```
[orders-dlq-demo] → Consumer → DefaultErrorHandler (retry) ──fail──→ [orders-dlq-demo.DLT]
                       ↓ success                                              ↓
                   next message                              dlq-handler + alerting-service
```

**Что в этой версии сделано реалистичнее:**
- Вместо ручного подсчёта retry — стандартные `DefaultErrorHandler` +
  `DeadLetterPublishingRecoverer` (идиоматичный Spring Kafka)
- Типизированный `OrderEvent`; два вида сбоев:
  - **poison** (`PoisonMessageException`) — заказы покупателей `bad-*`, non-retryable, сразу в DLQ
  - **transient** (`FailureSimulator`, ~20%) — повторяемые, часть выживает после retry
- DLQ-топик читают **два независимых потребителя**:
  - `dlq-handler` — сохраняет проблемные сообщения с метаданными сбоя для разбора
  - `alerting-service` — считает алерты по типу ошибки (новый сервис)

---

## Запуск

```bash
docker-compose up -d zookeeper kafka kafka-ui
docker-compose --profile case5 up -d

docker logs -f case5-consumer &
docker logs -f case5-dlq-handler &
```

| Сервис | Порт |
|---|---|
| case5-producer | 8088 |
| case5-consumer | 8092 |
| case5-dlq-handler | 8093 |
| case5-alerting-service | 8094 |

---

## Сценарий проверки

### Шаг 1: Нормальные заказы

```bash
curl -X POST "http://localhost:8088/api/orders/random?count=20"
curl http://localhost:8092/api/processing-stats
```

### Шаг 2: «Ядовитые» заказы → DLQ

```bash
curl -X POST "http://localhost:8088/api/orders/poison?count=3"
```
Логи consumer:
```
Sending record to DLQ: topic=orders-dlq-demo partition=.. offset=.. cause=Poison order ...
```
Логи dlq-handler:
```
╔═══════════════════════════════════════════╗
║  DLQ MESSAGE — REQUIRES ATTENTION
║  Order: ord-...   Customer: bad-customer
║  Error: ...PoisonMessageException: Poison order ...
╚═══════════════════════════════════════════╝
```

### Шаг 3: Проверить DLQ и алерты

```bash
curl http://localhost:8093/api/dlq      # сохранённые проблемные сообщения
curl http://localhost:8094/api/alerts   # счётчики алертов по типам ошибок
```

### Шаг 4: Kafka UI

http://localhost:8080 →
- `orders-dlq-demo` — основной топик
- `orders-dlq-demo.DLT` — только упавшие сообщения
- Consumer Groups → `dlq-consumer-group` → lag = 0 (топик не застрял)

---

## Тесты

```bash
cd case-5-dlq/consumer
mvn test
```

`DlqConsumerIntegrationTest` поднимает Embedded Kafka, отправляет нормальные и
«ядовитый» заказ и проверяет, что poison-сообщение оказалось в `orders-dlq-demo.DLT`.

---

## Политика повторов

| Тип ошибки | Поведение |
|---|---|
| `PoisonMessageException` | non-retryable — сразу в DLQ |
| `TransientProcessingException` и прочие | 3 повтора (`FixedBackOff` 1с), затем DLQ |

`DeadLetterPublishingRecoverer` автоматически добавляет в DLQ-сообщение заголовки:
исходный топик/партиция/оффсет, класс и текст исключения (`KafkaHeaders.DLT_*`).
