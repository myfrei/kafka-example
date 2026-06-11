# Case 7 (live-coding) — Non-blocking retries через retry-топики

Каркас для live-coding на интервью. Бизнес-обвязка и конфигурация уже готовы — нужно реализовать
только Kafka-логику в местах, помеченных `TODO (N)`. Полное условие и критерии — в
[../interview/03-case-7-livecoding.md](../interview/03-case-7-livecoding.md) (раздел с эталонным
решением предназначен интервьюеру).

## Что нужно сделать

Реализовать **НЕблокирующие повторы**: упавший платёж уходит в отдельный retry-топик с задержкой
(основной поток не блокируется), poison-платёж (`customerId` начинается с `bad-`) — сразу в DLT без
повторов. Это контраст к Case 5, где `DefaultErrorHandler` делает блокирующие ретраи.

```
payments-retry-demo
  ├─ success ─────────► обработан
  ├─ transient fail ─► payments-retry-demo-retry-0 ─► -retry-1 ─► -retry-2 ─► (успех / DLT)
  └─ poison (bad-*) ─────────────────────────────► payments-retry-demo-dlt   (без повторов)
```

## Чек-лист TODO

| # | Файл | Что сделать |
|---|---|---|
| 1 | `producer/.../service/PaymentProducerService.java` | Отправить платёж в топик с ключом `customerId` |
| 3 | `consumer/.../service/PaymentProcessingService.java` | В `process()`: poison → `PoisonMessageException`, иначе `failureSimulator.maybeFail(...)` |
| 4a | `consumer/.../Case7ConsumerApplication.java` | Включить инфраструктуру: `@EnableKafkaRetryTopic` |
| 4 | `consumer/.../listener/PaymentRetryListener.java` | `@KafkaListener` + `@RetryableTopic` (attempts, backoff, exclude poison) |
| 5 | `consumer/.../listener/PaymentRetryListener.java` | `@DltHandler` — прочитать заголовки сбоя, `registerDlt()` |
| 6 | `consumer/src/test/.../PaymentRetryIntegrationTest.java` | Тест: poison → DLT, нормальный обработан |

> (TODO 2 — REST-эндпоинты продюсера — уже готовы; нумерация сохранена по тексту условия.)
> Всё, что помечено `[ГОТОВО]`, трогать не нужно.

## Сборка и запуск

Каркас компилируется сразу (TODO-методы кидают `UnsupportedOperationException` / тест помечен `fail`):

```bash
# Скомпилировать
cd case-7-live-coding/producer && mvn -q compile
cd ../consumer && mvn -q test-compile

# Тест (после реализации TODO 6)
cd case-7-live-coding/consumer && mvn test

# Локальный запуск (нужен поднятый Kafka на localhost:9092)
docker-compose up -d zookeeper kafka kafka-ui
cd case-7-live-coding/producer && mvn spring-boot:run   # :8080
cd case-7-live-coding/consumer && mvn spring-boot:run   # :8080 (запускать на другом порту/в другом окне)
```

### Через Docker (опционально)

Добавьте в корневой `docker-compose.yml` профиль `case7`:

```yaml
  case7-producer:
    build: ./case-7-live-coding/producer
    container_name: case7-producer
    networks: [kafka-net]
    depends_on: { kafka: { condition: service_healthy } }
    ports: ["8097:8080"]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9093
    profiles: ["case7"]

  case7-consumer:
    build: ./case-7-live-coding/consumer
    container_name: case7-consumer
    networks: [kafka-net]
    depends_on: { kafka: { condition: service_healthy } }
    ports: ["8098:8080"]
    environment:
      KAFKA_BOOTSTRAP_SERVERS: kafka:9093
      APP_CHAOS_FAILURE_RATE: "0.3"
    profiles: ["case7"]
```

```bash
docker-compose --profile case7 up -d --build
docker logs -f case7-consumer
```

## Как проверить (после реализации)

```bash
# обычные платежи — обрабатываются
curl -X POST "http://localhost:8097/api/payments/random?count=20"
# poison — сразу в DLT, без повторов
curl -X POST "http://localhost:8097/api/payments/poison?count=3"
# счётчики
curl http://localhost:8098/api/retry-stats        # {"processed":..,"dlt":..}
```

В Kafka UI (http://localhost:8080) должны появиться топики:
`payments-retry-demo`, `payments-retry-demo-retry-0`, `-retry-1`, `-retry-2`, `payments-retry-demo-dlt`.

## Критерии приёмки

- [ ] Создаются retry- и dlt-топики (значит `@EnableKafkaRetryTopic` + `@RetryableTopic` подключены).
- [ ] Poison-платежи попадают в `payments-retry-demo-dlt` **без повторов** (нет их записей в retry-топиках).
- [ ] При `APP_CHAOS_FAILURE_RATE>0` transient-платежи проходят через retry-топики, часть выживает.
- [ ] Основной поток не блокируется: новые платежи обрабатываются, пока упавшие «отлёживаются» в retry.
- [ ] `@DltHandler` логирует метаданные сбоя из заголовков; `/api/retry-stats` корректен.
- [ ] `mvn test` зелёный.

## Подсказки

- `@RetryableTopic`/`@DltHandler` уже в `spring-kafka` — доп. зависимостей не нужно.
- Число retry-топиков = `attempts − 1` (при `attempts=4` → 3 retry + 1 dlt).
- В `@DltHandler` для retry-топиков заголовки — `KafkaHeaders.ORIGINAL_TOPIC` / `EXCEPTION_FQCN` /
  `EXCEPTION_MESSAGE` (НЕ `DLT_*`, как у standalone `DeadLetterPublishingRecoverer` в Case 5).
- `exclude = { PoisonMessageException.class }` — иначе poison будет зря гонять все повторы.
- Если retry/dlt-топики не появились — почти всегда забыт `@EnableKafkaRetryTopic` (TODO 4a).
