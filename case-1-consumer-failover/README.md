# Case 1 — Consumer Failover (2 Producers + 2 Consumers)

## Описание кейса

Демонстрирует поведение Kafka при потере одного из консьюмеров в группе.

**Ключевые концепции:**
- **Consumer Group** — все инстансы с одним `group-id` совместно читают топик
- **Partition Assignment** — Kafka делит партиции между консьюмерами в группе (один раздел — один консьюмер)
- **Rebalancing** — при падении консьюмера его партиции автоматически перераспределяются между живыми
- **Session Timeout** — через `session.timeout.ms` Kafka понимает, что консьюмер умер

**Топология:**
```
Producer-1 ──┐
             ├──→ [orders: p0, p1, p2] ──→ Consumer-1 (partition 0, 1)
Producer-2 ──┘                         ──→ Consumer-2 (partition 2)  ← падает
                                        ──→ Consumer-1 получает ВСЕ после rebalancing
```

---

## Запуск

```bash
# 1. Поднять инфраструктуру
docker-compose up -d zookeeper kafka kafka-ui

# 2. Запустить кейс 1
docker-compose --profile case1 up -d

# 3. Смотреть логи обоих консьюмеров
docker logs -f case1-consumer-1
docker logs -f case1-consumer-2
```

---

## Сценарий проверки

### Шаг 1: Оба консьюмера работают

```bash
# Смотрим логи consumer-1 — видим какие партиции ему назначены
docker logs -f case1-consumer-1
```

Ожидаемый вывод при старте:
```
[consumer-1] REBALANCING COMPLETE — Partitions assigned: [partition-0, partition-1]
[consumer-2] REBALANCING COMPLETE — Partitions assigned: [partition-2]
```

### Шаг 2: Отправить тестовые сообщения вручную

```bash
# Отправляем с разными ключами — разные партиции
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{"key": "order-aaa", "message": "{\"orderId\": \"order-aaa\", \"product\": \"apple\"}"}'

curl -X POST http://localhost:8082/api/orders \
  -H "Content-Type: application/json" \
  -d '{"key": "order-bbb", "message": "{\"orderId\": \"order-bbb\", \"product\": \"banana\"}"}'
```

Наблюдаем: сообщения с разными ключами попадают в разные партиции и читаются разными консьюмерами.

### Шаг 3: Симулировать падение consumer-2

```bash
docker stop case1-consumer-2
```

Ожидаемый вывод в логах consumer-1 через ~10 секунд:
```
[consumer-1] REBALANCING START — Partitions revoked: [partition-0, partition-1]
[consumer-1] REBALANCING COMPLETE — Partitions assigned: [partition-0, partition-1, partition-2]
```

Consumer-1 теперь читает ВСЕ 3 партиции.

### Шаг 4: Проверить отсутствие потерь сообщений

```bash
# Пока consumer-2 был выключен — producer-ы продолжали работать
# После rebalancing consumer-1 дочитал все накопленные сообщения
# Проверяем через Kafka UI: http://localhost:8080
```

### Шаг 5: Вернуть consumer-2

```bash
docker start case1-consumer-2
```

Ожидаемый вывод: снова произойдёт rebalancing, партиции снова разделятся.

---

## Что наблюдаем в Kafka UI (http://localhost:8080)

- **Topics → orders → Partitions** — сколько сообщений в каждой партиции
- **Consumer Groups → order-group → Members** — какой консьюмер читает какую партицию
- **Consumer Groups → order-group → Consumer Lag** — отставание консьюмера

---

## Параметры, влияющие на Rebalancing

| Параметр | Значение | Назначение |
|---|---|---|
| `session.timeout.ms` | 10000 | Через 10с без heartbeat Kafka считает консьюмера мёртвым |
| `heartbeat.interval.ms` | 3000 | Как часто консьюмер шлёт heartbeat (должен быть < session.timeout/3) |
| `max.poll.interval.ms` | 30000 | Максимальное время между вызовами poll() |

---

## Ключевые наблюдения

1. **Никакие сообщения не теряются** — Kafka хранит их до тех пор, пока не будут прочитаны
2. **Rebalancing занимает время** — в этот период консьюмеры не читают (stop-the-world)
3. **Порядок сообщений** — гарантируется только внутри одной партиции
4. **Равномерность** — Kafka старается распределить партиции равномерно между консьюмерами
