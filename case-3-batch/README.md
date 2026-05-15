# Case 3 — Batch Processing + Manual Offset

## Описание кейса

Демонстрирует чтение сообщений батчами и ручной сдвиг оффсета после обработки всего батча.

**Когда использовать батч-обработку:**
- Нужен bulk INSERT в БД (в 10-100x быстрее построчной вставки)
- Агрегация/дедупликация сообщений перед обработкой
- Вызов внешнего API с поддержкой batch-endpoints
- Высокий throughput при допустимой latency

**Ключевые параметры:**
- `max.poll.records=50` — максимум 50 сообщений за один `poll()`
- `fetch.min.bytes=1024` — ждать минимум 1KB данных перед возвратом poll()
- `fetch.max.wait.ms=500` — максимальное ожидание набора батча
- `setBatchListener(true)` — listener получает `List<ConsumerRecord>` вместо одной записи

---

## Запуск

```bash
docker-compose up -d zookeeper kafka kafka-ui
docker-compose --profile case3 up -d
docker logs -f case3-batch-consumer
```

---

## Сценарий проверки

### Шаг 1: Загрузить батч сообщений

```bash
# Отправить 100 сообщений подряд через producer
for i in {1..100}; do
  curl -s -X POST http://localhost:8084/api/messages \
    -H "Content-Type: application/json" \
    -d "{\"key\": \"key-$i\", \"message\": \"message #$i\"}" &
done
wait
echo "All 100 messages sent"
```

### Шаг 2: Наблюдать батч-обработку в логах

Ожидаемый вывод:
```
=== BATCH RECEIVED: 50 messages ===
  Partition 0: 17 records, offsets [0 - 16]
  Partition 1: 18 records, offsets [0 - 17]
  Partition 2: 15 records, offsets [0 - 14]
After deduplication: 50 unique keys (from 50 records)
=== BATCH COMMITTED: 50 messages processed ===

=== BATCH RECEIVED: 50 messages ===
...
=== BATCH COMMITTED: 50 messages processed ===
```

### Шаг 3: Проверить Consumer Lag в Kafka UI

1. Открыть http://localhost:8080
2. Consumer Groups → batch-consumer-group → Consumer Lag
3. Наблюдать как lag уменьшается батчами по 50

### Шаг 4: Сравнение скорости обработки

```bash
# Посчитать throughput в логах:
# Время на 50 сообщений = batch_size * 10ms = 500ms
# Throughput = 50 / 0.5 = 100 msg/sec

# Для сравнения: построчная обработка с отдельным коммитом = ~10ms * 50 = 500ms
# + 50 коммитов = значительно медленнее
```

---

## Движение оффсета в батче

```
poll() → [msg:p0:offset5, msg:p1:offset3, msg:p0:offset6, ...]
           ↓
       processBatch(records)  — обрабатываем ВСЕ сообщения
           ↓
       acknowledge()          — Kafka коммитит максимальный offset для каждой партиции:
                                partition-0: offset 6
                                partition-1: offset 3
                                partition-2: ...
```

**Если упасть на середине батча:**
- Оффсет не закоммичен
- При следующем старте весь батч перечитается снова
- Нужна идемпотентная обработка (дедупликация по ключу)

---

## Сравнение стратегий коммита

| Стратегия | Производительность | Гарантии |
|---|---|---|
| Auto commit (каждые N ms) | Высокая | Возможна потеря |
| Per-record manual | Низкая | At-least-once |
| **Per-batch manual** (этот кейс) | **Высокая** | **At-least-once** |
| Per-partition in batch | Средняя | At-least-once |
