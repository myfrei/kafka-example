# Case 1 — Consumer Failover (2 Producers + 2 Consumers + Analytics)

## Описание кейса

Демонстрирует поведение Kafka при потере одного из консьюмеров в группе.

**Ключевые концепции:**
- **Consumer Group** — все инстансы с одним `group-id` совместно читают топик
- **Partition Assignment** — Kafka делит партиции между консьюмерами в группе (один раздел — один консьюмер)
- **Rebalancing** — при падении консьюмера его партиции автоматически перераспределяются между живыми
- **Session Timeout** — через `session.timeout.ms` Kafka понимает, что консьюмер умер
- **Несколько consumer group на один топик** — analytics-service читает тот же топик независимо

**Что в этой версии сделано реалистичнее:**
- В топик `orders` публикуется типизированный `OrderEvent` (заказ с позициями, регионом, суммой) — сериализация через `JsonSerializer`/`JsonDeserializer`, а не сырая строка
- `FailureSimulator` роняет ~20% сообщений с transient-ошибкой → `DefaultErrorHandler` повторяет их 3 раза
- Добавлен **analytics-service** — отдельный потребитель в своей группе `order-analytics-group`, агрегирует выручку по регионам

**Топология:**
```
Producer-1 ──┐                        ┌─→ order-processing-group ─→ Consumer-1 / Consumer-2 (failover)
             ├──→ [orders: p0,p1,p2] ─┤
Producer-2 ──┘                        └─→ order-analytics-group  ─→ analytics-service (вся выручка)
```

---

## Запуск

```bash
docker-compose up -d zookeeper kafka kafka-ui
docker-compose --profile case1 up -d

docker logs -f case1-consumer-1
docker logs -f case1-consumer-2
```

| Сервис | Порт | Назначение |
|---|---|---|
| case1-producer-1 / -2 | 8081 / 8082 | REST API продюсеров |
| case1-analytics-service | 8086 | REST API аналитики |

---

## Сценарий проверки

### Шаг 1: Оба консьюмера работают

```bash
docker logs -f case1-consumer-1
```
```
[consumer-1] REBALANCING COMPLETE — Partitions assigned: [partition-0, partition-1]
[consumer-2] REBALANCING COMPLETE — Partitions assigned: [partition-2]
```

### Шаг 2: Отправить заказы вручную

```bash
# Сгенерировать 20 случайных заказов
curl -X POST "http://localhost:8081/api/orders/random?count=20"

# Или отправить конкретный заказ
curl -X POST http://localhost:8082/api/orders \
  -H "Content-Type: application/json" \
  -d '{
        "orderId": "ord-demo-1",
        "customerId": "cust-42",
        "region": "EU-WEST",
        "lines": [{"sku":"SKU-PHONE","name":"Smartphone X","quantity":1,"unitPrice":899.00}],
        "totalAmount": 899.00,
        "producerId": "manual",
        "createdAt": "2026-05-15T10:00:00Z"
      }'
```

### Шаг 3: Симулировать падение consumer-2

```bash
docker stop case1-consumer-2
```
Через ~10с в логах consumer-1:
```
[consumer-1] REBALANCING COMPLETE — Partitions assigned: [partition-0, partition-1, partition-2]
```

### Шаг 4: Проверить статистику и аналитику

```bash
# Сколько заказов обработал каждый инстанс
curl http://localhost:8081/api/orders/health      # producer alive-check
# (consumer-инстансы порт наружу не публикуют — статистика видна в логах)

# Выручка по регионам — analytics-service видит ВСЕ заказы независимо от failover
curl http://localhost:8086/api/analytics
```

### Шаг 5: Вернуть consumer-2

```bash
docker start case1-consumer-2
```
Снова rebalancing — партиции снова делятся между двумя инстансами.

### Наблюдение за chaos-ошибками

В логах консьюмеров видно повторную доставку упавших сообщений:
```
Retries exhausted for record at partition=1 offset=12 — giving up: Chaos: simulated transient failure ...
```
Управляется переменной `APP_CHAOS_FAILURE_RATE` (по умолчанию `0.2`).

---

## Тесты

```bash
cd case-1-consumer-failover/consumer
mvn test
```

`OrderConsumerIntegrationTest` поднимает Embedded Kafka, публикует типизированные `OrderEvent`
и проверяет, что `@KafkaListener` десериализовал их и передал в бизнес-логику.

---

## Параметры, влияющие на Rebalancing

| Параметр | Значение | Назначение |
|---|---|---|
| `session.timeout.ms` | 10000 | Через 10с без heartbeat Kafka считает консьюмера мёртвым |
| `heartbeat.interval.ms` | 3000 | Как часто консьюмер шлёт heartbeat (должен быть < session.timeout/3) |
| `max.poll.interval.ms` | 30000 | Максимальное время между вызовами poll() |

---

## Ключевые наблюдения

1. **Никакие сообщения не теряются** — Kafka хранит их, пока не будут прочитаны
2. **Rebalancing занимает время** — в этот период консьюмеры не читают (stop-the-world)
3. **Порядок сообщений** — гарантируется только внутри одной партиции
4. **Группы независимы** — failover в `order-processing-group` не влияет на `order-analytics-group`
