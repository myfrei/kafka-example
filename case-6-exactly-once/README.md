# Case 6 — Exactly-Once Semantics (EOS)

## Описание кейса

Демонстрирует идемпотентный продюсер, транзакционную отправку и идемпотентный консьюмер.

**Уровни семантики Kafka:**

| Семантика | Описание | Риск |
|---|---|---|
| At-most-once | Сообщение может потеряться | Потеря данных |
| At-least-once | Сообщение придёт минимум раз | Дубли |
| **Exactly-once** | Строго один раз | Сложность настройки |

**Компоненты EOS:**
1. **Idempotent Producer** (`enable.idempotence=true`) — дедупликация на уровне broker
2. **Transactional Producer** (`transactional.id`) — атомарная отправка пачки сообщений
3. **Isolated Consumer** (`isolation.level=read_committed`) — видит только закоммиченные транзакции
4. **Idempotent Consumer** (дедупликация по messageId в PostgreSQL) — защита от дублей на стороне консьюмера

---

## Запуск

```bash
docker-compose up -d zookeeper kafka kafka-ui postgres
docker-compose --profile case6 up -d

docker logs -f case6-producer &
docker logs -f case6-consumer &
```

---

## Сценарий проверки

### Шаг 1: Успешная транзакционная отправка

```bash
curl -X POST http://localhost:8089/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "orderId": "order-001",
    "items": ["apple", "banana", "cherry"]
  }'
```

В консьюмере появятся 5 сообщений (header + 3 items + footer) атомарно:
```
[consumer] Received: messageId=exactly-once-topic:0:0 key=order-001
[consumer] Business logic: {"type":"ORDER_HEADER", "orderId":"order-001", ...}
[consumer] Received: messageId=exactly-once-topic:0:1 key=order-001-item-0
...
```

### Шаг 2: Откат транзакции

```bash
curl -X POST http://localhost:8089/api/orders/fail \
  -H "Content-Type: application/json" \
  -d '{"orderId": "order-fail"}'
```

Ожидаемое поведение:
- Продюсер отправил 2 сообщения
- Бросил исключение → транзакция откатилась
- Консьюмер с `read_committed` НЕ увидел ни одного из этих сообщений

```bash
# Проверяем — в таблице нет этих сообщений
docker exec -it postgres psql -U kafka_user -d kafka_demo \
  -c "SELECT * FROM processed_messages WHERE payload LIKE '%WILL_ROLLBACK%';"
# → 0 rows
```

### Шаг 3: Проверка идемпотентности (защита от дублей)

```bash
# Симулируем получение того же сообщения дважды
# (в реальности это происходит при rebalancing)
# Смотрим в таблице — один и тот же messageId обработан только раз:
docker exec -it postgres psql -U kafka_user -d kafka_demo \
  -c "SELECT message_id, COUNT(*) FROM processed_messages GROUP BY message_id HAVING COUNT(*) > 1;"
# → 0 rows (нет дублей!)
```

### Шаг 4: Проверить в Kafka UI

- Topics → exactly-once-topic → Messages
- Обратить внимание: видны только закоммиченные транзакции
- Consumer Groups → exactly-once-group — нет lag

---

## Конфигурация: ключевые различия

```yaml
# Обычный продюсер
acks: 1
enable.idempotence: false

# Идемпотентный продюсер (защита от дублей при retry)
acks: all
enable.idempotence: true
max.in.flight.requests.per.connection: 5

# Транзакционный продюсер (атомарность)
transactional.id: my-app-tx
enable.idempotence: true   # обязательно при транзакциях

# Консьюмер: только закоммиченные транзакции
isolation.level: read_committed
```

---

## Таблица дедупликации в PostgreSQL

```sql
-- Посмотреть все обработанные сообщения
SELECT message_id, topic, partition_id, offset_value, LEFT(payload, 50), processed_at
FROM processed_messages
ORDER BY processed_at DESC
LIMIT 20;

-- Проверить наличие дублей
SELECT message_id, COUNT(*)
FROM processed_messages
GROUP BY message_id
HAVING COUNT(*) > 1;
```
