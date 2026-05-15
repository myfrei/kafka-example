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
4. **Idempotent Consumer** (дедупликация по messageId в PostgreSQL) — защита от дублей

**Что в этой версии сделано реалистичнее:**
- Заказ публикуется типизированными `OrderMessage` (ORDER_HEADER → ORDER_ITEM* → ORDER_FOOTER)
- Консьюмер пишет дедуп-запись (`processed_messages`) и бизнес-данные (`orders`) в одной
  транзакции БД; `FailureSimulator` (~15%) откатывает транзакцию — проверяется идемпотентность
- Добавлен **shipping-service** — downstream-потребитель с `read_committed`,
  отгружает заказы только из закоммиченных транзакций

**Топология:**
```
Producer (tx) → [exactly-once-topic] ─┬─→ exactly-once-group → Consumer → PostgreSQL (дедуп + orders)
                                      └─→ shipping-group     → shipping-service (отгрузка)
```

---

## Запуск

```bash
docker-compose up -d zookeeper kafka kafka-ui postgres
docker-compose --profile case6 up -d

docker logs -f case6-producer &
docker logs -f case6-consumer &
```

| Сервис | Порт |
|---|---|
| case6-producer | 8089 |
| case6-consumer | 8095 |
| case6-shipping-service | 8096 |

---

## Сценарий проверки

### Шаг 1: Успешная транзакционная отправка

```bash
curl -X POST http://localhost:8089/api/orders \
  -H "Content-Type: application/json" \
  -d '{"orderId": "order-001", "items": ["apple", "banana", "cherry"]}'
```
Консьюмер получит 5 сообщений (header + 3 items + footer) атомарно.

```bash
curl http://localhost:8095/api/processed     # обработанные сообщения и заказы
curl http://localhost:8096/api/shipments     # отгрузка
```

### Шаг 2: Откат транзакции

```bash
curl -X POST http://localhost:8089/api/orders/fail \
  -H "Content-Type: application/json" \
  -d '{"orderId": "order-fail"}'
```
Продюсер отправил 2 сообщения и бросил исключение → транзакция откатилась.
Консьюмер с `read_committed` НЕ увидел их:
```bash
docker exec -it postgres psql -U kafka_user -d kafka_demo \
  -c "SELECT COUNT(*) FROM orders WHERE order_id = 'order-fail';"
# → 0
```

### Шаг 3: Проверка идемпотентности

```bash
docker exec -it postgres psql -U kafka_user -d kafka_demo \
  -c "SELECT message_id, COUNT(*) FROM processed_messages GROUP BY message_id HAVING COUNT(*) > 1;"
# → 0 rows (нет дублей, даже при сбоях и повторной доставке)
```

### Шаг 4: Kafka UI

http://localhost:8080 → Topics → `exactly-once-topic` — видны только закоммиченные транзакции.

---

## Тесты

```bash
cd case-6-exactly-once/consumer
mvn test
```

`IdempotentConsumerIntegrationTest` поднимает Embedded Kafka + H2: коммитит два
заказа и откатывает один, проверяя, что в БД попали ровно сообщения закоммиченных
транзакций (8 записей), а откатившийся заказ невидим.

---

## Конфигурация: ключевые различия

```yaml
# Транзакционный продюсер (атомарность)
transactional.id: case6-tx-*
enable.idempotence: true   # обязательно при транзакциях
acks: all

# Консьюмер: только закоммиченные транзакции
isolation.level: read_committed
```
