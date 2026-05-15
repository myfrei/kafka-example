# Kafka Training Project — Java Microservices

Учебный проект для изучения Apache Kafka с Spring Boot. Каждый кейс — отдельный runnable пример с полным описанием и способами проверки.

## Технологии

- **Java 17** · **Spring Boot 3.3** · **Spring Kafka**
- **Apache Kafka 7.6** (Confluent Platform)
- **PostgreSQL 16** (Cases 2, 6)
- **Kafka UI** (Provectus) — визуальный мониторинг
- **Docker Compose** — весь стек одной командой

---

## Структура проекта

```
kafka-example/
├── docker-compose.yml              ← Весь стек: Kafka + ZooKeeper + UI + PostgreSQL
├── init-db.sql                     ← Инициализация схемы БД
│
├── case-1-consumer-failover/       ← 2 Producer + 2 Consumer, ребалансировка
│   ├── producer/                   ← Spring Boot Producer (порт 8081/8082)
│   ├── consumer/                   ← Spring Boot Consumer с ConsumerSeekAware
│   └── README.md                   ← Описание + шаги проверки
│
├── case-2-manual-offset/           ← Ручные оффсеты + хранение в PostgreSQL
│   ├── producer/                   ← (порт 8083)
│   ├── consumer/                   ← ManualAck + ConsumerSeekAware + JPA
│   └── README.md
│
├── case-3-batch/                   ← Батч-чтение + ручной коммит оффсета
│   ├── producer/                   ← (порт 8084)
│   ├── consumer/                   ← BatchListener, max.poll.records=50
│   └── README.md
│
├── case-4-partitions/              ← 3 партиции × 3 инстанса сервиса
│   ├── producer/                   ← (порт 8085)
│   ├── service/                    ← Деплоится 3 раза (instance-1,2,3)
│   └── README.md
│
├── case-5-dlq/                     ← Dead Letter Queue
│   ├── producer/                   ← (порт 8088)
│   ├── consumer/                   ← Retry 3x → DLQ
│   ├── dlq-handler/                ← Обработчик DLQ
│   └── README.md
│
└── case-6-exactly-once/            ← Exactly-Once Semantics
    ├── producer/                   ← Transactional Producer (порт 8089)
    ├── consumer/                   ← Idempotent Consumer + PostgreSQL
    └── README.md
```

---

## Быстрый старт

### 1. Поднять инфраструктуру

```bash
# Запустить только инфраструктуру (Kafka, ZooKeeper, UI, PostgreSQL)
docker-compose up -d zookeeper kafka kafka-ui postgres

# Подождать ~30 секунд, проверить что всё поднялось
docker-compose ps

# Kafka UI доступен: http://localhost:8080
```

### 2. Запустить нужный кейс

```bash
# Case 1: Consumer Failover
docker-compose --profile case1 up -d

# Case 2: Manual Offset + PostgreSQL
docker-compose --profile case2 up -d

# Case 3: Batch Processing
docker-compose --profile case3 up -d

# Case 4: 3 Partitions × 3 Instances
docker-compose --profile case4 up -d

# Case 5: Dead Letter Queue
docker-compose --profile case5 up -d

# Case 6: Exactly-Once
docker-compose --profile case6 up -d
```

### 3. Остановить кейс

```bash
docker-compose --profile case1 down
```

---

## Кейсы

### Case 1 — Consumer Failover
**Что демонстрирует:** ребалансировка при падении консьюмера

**Сценарий:**
```bash
# Смотрим логи консьюмеров
docker logs -f case1-consumer-1 &
docker logs -f case1-consumer-2 &

# "Убиваем" один консьюмер
docker stop case1-consumer-2

# Наблюдаем: consumer-1 берёт ВСЕ партиции
# Возвращаем
docker start case1-consumer-2
# Наблюдаем: снова rebalancing, партиции делятся
```

**Ключевые параметры:**
- `session.timeout.ms=10000` — через 10с без heartbeat считается мёртвым
- `heartbeat.interval.ms=3000` — как часто шлёт heartbeat
- `group-id=order-group` — все инстансы в одной группе

---

### Case 2 — Manual Offset + PostgreSQL
**Что демонстрирует:** хранение оффсетов в БД, replay сообщений

**Сценарий:**
```bash
# Отправить сообщения
curl -X POST http://localhost:8083/api/orders \
  -H "Content-Type: application/json" \
  -d '{"key": "k1", "message": "test"}'

# Посмотреть оффсеты в БД
curl http://localhost:8083/api/offsets

# Сдвинуть оффсет на -1 (replay с начала)
curl -X POST http://localhost:8083/api/offsets/seek \
  -H "Content-Type: application/json" \
  -d '{"partition": 0, "offset": -1}'

# Перезапустить — консьюмер начнёт читать с начала
docker restart case2-consumer
```

**Напрямую в PostgreSQL:**
```sql
-- Посмотреть оффсеты
SELECT * FROM kafka_offsets;

-- Ручной сдвиг (replay)
UPDATE kafka_offsets SET offset_value = 0 WHERE topic = 'manual-offset-topic';
```

---

### Case 3 — Batch Processing
**Что демонстрирует:** чтение батчами, bulk-insert, коммит после батча

**Сценарий:**
```bash
# Загрузить 100 сообщений
for i in {1..100}; do
  curl -s -X POST http://localhost:8084/api/messages \
    -H "Content-Type: application/json" \
    -d '{"key":"k'$i'","message":"msg '$i'"}' &
done; wait

# Наблюдать в логах: батчи по 50 сообщений
docker logs -f case3-batch-consumer
```

**Ожидаемый вывод:**
```
=== BATCH RECEIVED: 50 messages ===
  Partition 0: 17 records, offsets [0 - 16]
  Partition 1: 18 records, offsets [0 - 17]
  After deduplication: 50 unique keys
=== BATCH COMMITTED: 50 messages processed ===
```

---

### Case 4 — 3 Partitions × 3 Instances
**Что демонстрирует:** параллельное чтение, роль ключа, распределение партиций

**Сценарий:**
```bash
# Отправить 30 сообщений с разными ключами
curl "http://localhost:8085/api/messages/flood?count=30"

# В логах видно: каждый инстанс читает только свою партицию
docker logs case4-service-1  # → только partition-0
docker logs case4-service-2  # → только partition-1
docker logs case4-service-3  # → только partition-2

# Принудительно в partition-2
curl -X POST http://localhost:8085/api/messages/partition/2 \
  -H "Content-Type: application/json" \
  -d '{"key":"forced","value":"test"}'
# → прочитает ТОЛЬКО instance-3

# Остановить instance-2
docker stop case4-service-2
# → instance-1 или 3 возьмёт partition-1 (rebalancing)
```

---

### Case 5 — Dead Letter Queue
**Что демонстрирует:** обработка ошибок, retry, DLQ-паттерн

**Сценарий:**
```bash
# Успешное сообщение
curl -X POST http://localhost:8088/api/orders \
  -H "Content-Type: application/json" \
  -d '{"key":"ok","message":"{\"orderId\":\"1\"}"}'

# Сообщение, которое уйдёт в DLQ (3 попытки, потом DLQ)
curl -X POST http://localhost:8088/api/orders \
  -H "Content-Type: application/json" \
  -d '{"key":"fail","message":"{\"fail\":true}"}'

# Наблюдаем в логах dlq-handler:
docker logs -f case5-dlq-handler

# В Kafka UI: Topics → orders.DLQ
```

---

### Case 6 — Exactly-Once Semantics
**Что демонстрирует:** транзакционный продюсер, идемпотентный консьюмер

**Сценарий:**
```bash
# Транзакционная отправка (header + items + footer атомарно)
curl -X POST http://localhost:8089/api/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ord-1","items":["apple","banana"]}'

# Откат транзакции (консьюмер НЕ увидит эти сообщения)
curl -X POST http://localhost:8089/api/orders/fail \
  -H "Content-Type: application/json" \
  -d '{"orderId":"ord-fail"}'

# Проверить в PostgreSQL — нет мусора от откатившейся транзакции
docker exec -it postgres psql -U kafka_user -d kafka_demo \
  -c "SELECT message_id, LEFT(payload,40) FROM processed_messages ORDER BY processed_at DESC LIMIT 10;"
```

---

## Полезные команды

### Kafka CLI

```bash
# Список топиков
docker exec kafka kafka-topics --bootstrap-server localhost:9093 --list

# Создать топик с 3 партициями
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9093 \
  --create --topic my-topic --partitions 3 --replication-factor 1

# Описание топика (партиции, leader, replicas)
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9093 \
  --describe --topic orders

# Посмотреть consumer groups
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9093 --list

# Описание группы (lag, assigned partitions)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9093 \
  --describe --group order-group

# Сбросить оффсет группы на начало
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9093 \
  --group order-group \
  --topic orders \
  --reset-offsets --to-earliest --execute

# Сбросить оффсет на конкретную позицию
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9093 \
  --group order-group \
  --topic orders:0 \
  --reset-offsets --to-offset 42 --execute

# Прочитать сообщения из топика
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9093 \
  --topic orders \
  --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true

# Отправить тестовое сообщение из консоли
docker exec -it kafka kafka-console-producer \
  --bootstrap-server localhost:9093 \
  --topic orders \
  --property parse.key=true \
  --property key.separator=:
# Затем вводить: key:value
```

### PostgreSQL

```bash
# Подключиться к БД
docker exec -it postgres psql -U kafka_user -d kafka_demo

# Полезные запросы внутри psql:
\dt                                    -- список таблиц
SELECT * FROM kafka_offsets;           -- оффсеты (Case 2)
SELECT * FROM processed_messages;      -- обработанные сообщения (Case 6)
SELECT * FROM orders;                  -- бизнес-данные (Case 6)
```

### Мониторинг

```bash
# Kafka UI
open http://localhost:8080

# Consumer lag всех групп
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9093 --list | \
  xargs -I{} docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9093 --describe --group {}

# Метрики сервисов (Spring Actuator)
curl http://localhost:8081/actuator/health
curl http://localhost:8081/actuator/metrics
```

---

## Карта концепций

| Концепция | Где смотреть | Ключевой параметр |
|---|---|---|
| Consumer Group | Case 1 | `group-id` |
| Partition Assignment | Case 1, 4 | `ConsumerSeekAware.onPartitionsAssigned` |
| Rebalancing | Case 1, 4 | `session.timeout.ms` |
| Manual Commit | Case 2, 3 | `enable-auto-commit: false` |
| Offset Storage (DB) | Case 2 | `ConsumerSeekAware` + PostgreSQL |
| Batch Processing | Case 3 | `setBatchListener(true)`, `max.poll.records` |
| Parallel Partitions | Case 4 | `partitions=3`, 3 инстанса |
| Key-based Routing | Case 4 | `murmur2(key) % partitions` |
| DLQ Pattern | Case 5 | `DeadLetterPublishingRecoverer` |
| Idempotent Producer | Case 6 | `enable.idempotence=true` |
| Transactions | Case 6 | `transactional.id`, `@Transactional` |
| Read Committed | Case 6 | `isolation.level=read_committed` |
| Idempotent Consumer | Case 6 | Дедупликация по messageId в БД |
