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

**Что в этой версии сделано реалистичнее:**
- В батч приходит типизированный `OrderEvent` (заказ с позициями)
- После успешного коммита батча консьюмер публикует `BatchSummary` в топик `batch-summary`
- Добавлен **reporting-service** — downstream-потребитель, агрегирующий сводки в отчёт
- `FailureSimulator` роняет ~15% батчей целиком — наглядная демонстрация
  риска батч-обработки «всё или ничего»

**Топология:**
```
Producer → [batch-topic] → Batch Consumer → [batch-summary] → reporting-service
                            (батчи по 50)    (сводка батча)    (накопительный отчёт)
```

---

## Запуск

```bash
docker-compose up -d zookeeper kafka kafka-ui
docker-compose --profile case3 up -d
docker logs -f case3-batch-consumer
```

| Сервис | Порт |
|---|---|
| case3-producer | 8084 |
| case3-reporting-service | 8090 |

---

## Сценарий проверки

### Шаг 1: Залить заказы

```bash
curl "http://localhost:8084/api/messages/flood?count=200"
```

### Шаг 2: Наблюдать батч-обработку в логах

```
=== BATCH RECEIVED: 50 records ===
  Partition 0: 17 records, offsets [0 - 16]
  Partition 1: 18 records, offsets [0 - 17]
  Partition 2: 15 records, offsets [0 - 14]
Batch processed: 50 orders, 48 distinct customers, revenue=...
=== BATCH COMMITTED: 50 records, summary batch-xxxxxxxx published ===
```

При сбое (chaos):
```
Batch processing failed (50 records). NOT committing offset — batch will be re-read ...
```

### Шаг 3: Проверить статистику и отчёт

```bash
# Сколько записей/батчей обработал консьюмер
curl http://localhost:8084 >/dev/null   # producer alive
# reporting-service — накопительный отчёт по сводкам батчей
curl http://localhost:8090/api/report
```

### Шаг 4: Consumer Lag в Kafka UI

http://localhost:8080 → Consumer Groups → `batch-consumer-group` → Consumer Lag —
видно, как lag уменьшается батчами по 50.

---

## Тесты

```bash
cd case-3-batch/consumer
mvn test
```

`BatchConsumerIntegrationTest` поднимает Embedded Kafka, заливает 60 заказов
и проверяет, что они прочитаны и обработаны батчами.

---

## Движение оффсета в батче

```
poll() → [50 записей] → processBatch() → acknowledge() → Kafka коммитит
                                                          max offset каждой партиции
```

**Если упасть на середине батча:** оффсет не закоммичен, весь батч перечитается —
нужна идемпотентная обработка (дедупликация по ключу, см. `BatchProcessingService.deduplicate`).

---

## Сравнение стратегий коммита

| Стратегия | Производительность | Гарантии |
|---|---|---|
| Auto commit (каждые N ms) | Высокая | Возможна потеря |
| Per-record manual | Низкая | At-least-once |
| **Per-batch manual** (этот кейс) | **Высокая** | **At-least-once** |
| Per-partition in batch | Средняя | At-least-once |
