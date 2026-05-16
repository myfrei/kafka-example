# Kafka Training Project — Java Microservices

Учебный проект для изучения Apache Kafka с Spring Boot. Каждый кейс — отдельный
runnable пример одного концепта Kafka, с типизированной доменной моделью,
инъекцией случайных сбоев и интеграционными тестами.

## Технологии

- **Java 21** · **Spring Boot 3.5** · **Spring Kafka**
- **Apache Kafka** (Confluent Platform 7.9)
- **PostgreSQL 17** (Cases 2, 6)
- **Kafka UI** (Provectus) — визуальный мониторинг
- **Docker Compose** — весь стек одной командой
- **Embedded Kafka + H2** — интеграционные тесты без Docker

---

## Структура проекта

Каждый кейс состоит из независимых Maven-модулей (общего родительского pom нет).
Помимо producer/consumer в каждый кейс добавлен отдельный downstream-сервис.

```
kafka-example/
├── docker-compose.yml              ← Весь стек: Kafka + ZooKeeper + UI + PostgreSQL
├── init-db.sql                     ← Схема БД (kafka_offsets, case2_processed_orders,
│                                     processed_messages, orders)
│
├── case-1-consumer-failover/       ← Ребалансировка consumer group
│   ├── producer/                   ← (порт 8081/8082)
│   ├── consumer/                   ← ConsumerSeekAware + retry, 2 инстанса
│   └── analytics-service/          ← (порт 8086) выручка по регионам
│
├── case-2-manual-offset/           ← Ручные оффсеты в PostgreSQL
│   ├── producer/                   ← (порт 8083)
│   ├── consumer/                   ← manual ack, order+offset атомарно
│   └── audit-service/              ← (порт 8087) журнал аудита
│
├── case-3-batch/                   ← Батч-чтение
│   ├── producer/                   ← (порт 8084)
│   ├── consumer/                   ← BatchListener, max.poll.records=50
│   └── reporting-service/          ← (порт 8090) отчёт по сводкам батчей
│
├── case-4-partitions/              ← 3 партиции × 3 инстанса
│   ├── producer/                   ← (порт 8085)
│   ├── service/                    ← 3 инстанса (instance-1,2,3)
│   └── aggregator-service/         ← (порт 8091) агрегация по всем партициям
│
├── case-5-dlq/                     ← Dead Letter Queue
│   ├── producer/                   ← (порт 8088)
│   ├── consumer/                   ← (порт 8092) DefaultErrorHandler + DLQ
│   ├── dlq-handler/                ← (порт 8093) разбор DLQ
│   └── alerting-service/           ← (порт 8094) алерты по DLQ
│
└── case-6-exactly-once/            ← Exactly-Once Semantics
    ├── producer/                   ← (порт 8089) транзакционный продюсер
    ├── consumer/                   ← (порт 8095) идемпотентный консьюмер + PostgreSQL
    └── shipping-service/           ← (порт 8096) отгрузка (read_committed)
```

Подробные сценарии проверки — в `README.md` каждого кейса.

---

## Быстрый старт

```bash
# 1. Поднять инфраструктуру
docker-compose up -d zookeeper kafka kafka-ui postgres
docker-compose ps                       # подождать ~30с
# Kafka UI: http://localhost:8080

# 2. Запустить нужный кейс (профили case1..case6)
docker-compose --profile case1 up -d

# 3. Остановить кейс
docker-compose --profile case1 down
```

---

## Кейсы

| Кейс | Концепт | Ключевой механизм | Доп. сервис |
|---|---|---|---|
| 1 — Consumer Failover | Ребалансировка группы | `ConsumerSeekAware`, `session.timeout.ms`, retry | analytics-service |
| 2 — Manual Offset | Оффсеты в PostgreSQL, replay | manual ack, order+offset в одной транзакции | audit-service |
| 3 — Batch Processing | Чтение батчами | `setBatchListener(true)`, `max.poll.records` | reporting-service |
| 4 — Partitions | Параллельное чтение | 3 партиции × 3 инстанса, ключевое распределение | aggregator-service |
| 5 — Dead Letter Queue | Обработка ошибок | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` | alerting-service |
| 6 — Exactly-Once | Транзакции + идемпотентность | `transactional.id`, `read_committed`, дедуп в БД | shipping-service |

### Что общего у всех кейсов

- **Типизированная модель** — сообщения это Java-records (`OrderEvent`, `ActivityEvent`,
  `OrderMessage`, ...), сериализация через `JsonSerializer`/`JsonDeserializer`.
- **Инъекция сбоев** — `FailureSimulator` роняет часть сообщений с вероятностью
  `APP_CHAOS_FAILURE_RATE` (в docker-compose 0.1–0.25). Каждый кейс показывает
  свой путь восстановления: retry, replay, DLQ, откат транзакции.
- **Доп. сервис** — отдельная consumer group, демонстрирующая независимое
  потребление того же топика.

---

## Тесты

В основном consumer-модуле каждого кейса есть интеграционный тест на Embedded Kafka
(Docker не нужен). Кейсы 2 и 6 используют H2 in-memory вместо PostgreSQL.

```bash
cd case-1-consumer-failover/consumer && mvn test
cd case-2-manual-offset/consumer     && mvn test
cd case-3-batch/consumer             && mvn test
cd case-4-partitions/service         && mvn test
cd case-5-dlq/consumer               && mvn test
cd case-6-exactly-once/consumer      && mvn test
```

В Docker-образах тесты пропускаются (`-DskipTests`) — запускайте их отдельно через `mvn test`.

---

## Полезные команды

### Kafka CLI

```bash
# Список топиков
docker exec kafka kafka-topics --bootstrap-server localhost:9093 --list

# Описание топика (партиции, leader, replicas)
docker exec kafka kafka-topics --bootstrap-server localhost:9093 --describe --topic orders

# Consumer groups и их lag
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 --list
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 \
  --describe --group order-processing-group

# Сбросить оффсет группы на начало
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9093 \
  --group order-processing-group --topic orders --reset-offsets --to-earliest --execute

# Прочитать сообщения из топика
docker exec kafka kafka-console-consumer --bootstrap-server localhost:9093 \
  --topic orders --from-beginning --property print.key=true --property print.partition=true
```

### PostgreSQL

```bash
docker exec -it postgres psql -U kafka_user -d kafka_demo
# \dt                                  -- список таблиц
# SELECT * FROM kafka_offsets;          -- оффсеты (Case 2)
# SELECT * FROM case2_processed_orders; -- обработанные заказы (Case 2)
# SELECT * FROM processed_messages;     -- дедупликация (Case 6)
# SELECT * FROM orders;                 -- бизнес-данные (Case 6)
```

### Мониторинг

```bash
open http://localhost:8080             # Kafka UI
curl http://localhost:8086/api/analytics   # пример: аналитика Case 1
```

---

## Карта концепций

| Концепция | Где смотреть | Ключевой параметр |
|---|---|---|
| Consumer Group | Case 1 | `group-id` |
| Rebalancing | Case 1, 4 | `session.timeout.ms`, `ConsumerSeekAware` |
| Manual Commit | Case 2, 3 | `enable-auto-commit: false`, `AckMode.MANUAL_IMMEDIATE` |
| Offset Storage (DB) | Case 2 | `ConsumerSeekAware` + PostgreSQL |
| Batch Processing | Case 3 | `setBatchListener(true)`, `max.poll.records` |
| Key-based Routing | Case 4 | `murmur2(key) % partitions` |
| DLQ Pattern | Case 5 | `DeadLetterPublishingRecoverer` |
| Idempotent / Transactional | Case 6 | `enable.idempotence`, `transactional.id`, `read_committed` |
| Типизированная сериализация | Все | `JsonSerializer` / `JsonDeserializer` |
| Обработка сбоев | Все | `FailureSimulator`, `DefaultErrorHandler` |
