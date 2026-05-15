# Case 2 — Manual Offset Management + PostgreSQL

## Описание кейса

Демонстрирует ручное управление Kafka-оффсетами с хранением в PostgreSQL.

**Зачем это нужно?**
- Стандартные оффсеты хранятся в `__consumer_offsets` (внутренний Kafka-топик)
- Иногда нужна **атомарность**: сохранить бизнес-данные + оффсет в одной транзакции БД
- Нужна возможность **replay**: перечитать сообщения с произвольного места без kafka-consumer-groups.sh
- Нужно хранить **аудит**: кто, когда, с каким результатом обработал сообщение

**Ключевые настройки:**
- `enable-auto-commit: false` — Kafka не коммитит оффсет автоматически
- `AckMode.MANUAL_IMMEDIATE` — оффсет коммитится только при вызове `acknowledge()`
- `ConsumerSeekAware` — при старте читаем оффсет из БД и сдвигаем Kafka-курсор

---

## Запуск

```bash
# 1. Инфраструктура
docker-compose up -d zookeeper kafka kafka-ui postgres

# 2. Запустить кейс 2
docker-compose --profile case2 up -d

# 3. Смотреть логи
docker logs -f case2-consumer
```

---

## Сценарий проверки

### Шаг 1: Отправить сообщения

```bash
curl -X POST http://localhost:8083/api/orders \
  -H "Content-Type: application/json" \
  -d '{"key": "k1", "message": "first message"}'

curl -X POST http://localhost:8083/api/orders \
  -H "Content-Type: application/json" \
  -d '{"key": "k2", "message": "second message"}'

curl -X POST http://localhost:8083/api/orders \
  -H "Content-Type: application/json" \
  -d '{"key": "k3", "message": "third message"}'
```

### Шаг 2: Проверить сохранённые оффсеты в PostgreSQL

```bash
# Через REST API
curl http://localhost:8083/api/offsets

# Напрямую в БД
docker exec -it postgres psql -U kafka_user -d kafka_demo \
  -c "SELECT * FROM kafka_offsets;"
```

Ожидаемый вывод:
```
 topic               | partition_id | offset_value | consumer_group       | updated_at
---------------------+--------------+--------------+----------------------+--------------------
 manual-offset-topic |            0 |            2 | manual-offset-group  | 2024-01-01 10:00:00
```

### Шаг 3: Ручной сдвиг оффсета (Replay)

Хотим перечитать все сообщения с самого начала:

```bash
# Устанавливаем оффсет -1 (перед первым сообщением)
curl -X POST http://localhost:8083/api/offsets/seek \
  -H "Content-Type: application/json" \
  -d '{"partition": 0, "offset": -1}'

# Перезапускаем консьюмер
docker restart case2-consumer
```

Наблюдаем: консьюмер перечитает все сообщения с начала.

### Шаг 4: Симулировать ошибку обработки

В `ManualOffsetConsumer.processMessage()` добавить временно:
```java
throw new RuntimeException("Simulated failure");
```

Отправить сообщение → наблюдать: оффсет НЕ сохраняется в БД, сообщение перечитывается снова.

---

## Схема взаимодействия

```
Producer → [manual-offset-topic] → Consumer
                                       ↓
                                   processMessage()
                                       ↓
                                   saveOffset() ─── PostgreSQL
                                       ↓
                                   acknowledge() → Kafka (__consumer_offsets)

При следующем старте:
Consumer starts → onPartitionsAssigned() → read offset from PostgreSQL → seek()
```

---

## Таблица оффсетов в PostgreSQL

```sql
-- Посмотреть все оффсеты
SELECT * FROM kafka_offsets ORDER BY partition_id;

-- Сдвинуть оффсет вручную (replay последних 10 сообщений партиции 0)
UPDATE kafka_offsets
SET offset_value = offset_value - 10, updated_at = NOW()
WHERE topic = 'manual-offset-topic' AND partition_id = 0 AND consumer_group = 'manual-offset-group';

-- Сбросить на начало
UPDATE kafka_offsets SET offset_value = -1 WHERE topic = 'manual-offset-topic';
```

После изменений в БД — перезапустить `case2-consumer`.

---

## Разница в режимах AckMode

| Режим | Описание |
|---|---|
| `AUTO` | Коммит по расписанию (auto-commit-interval) |
| `BATCH` | Коммит после обработки целого batch poll() |
| `MANUAL` | `acknowledge()` при следующем poll() |
| `MANUAL_IMMEDIATE` | `acknowledge()` сразу (используем в этом кейсе) |
| `RECORD` | После каждого сообщения (автоматически) |
