# Case 2 — Manual Offset Management + PostgreSQL

## Описание кейса

Демонстрирует ручное управление Kafka-оффсетами с хранением в PostgreSQL.

**Зачем это нужно?**
- Стандартные оффсеты хранятся в `__consumer_offsets` (внутренний Kafka-топик)
- Иногда нужна **атомарность**: сохранить бизнес-данные + оффсет в одной транзакции БД
- Нужна возможность **replay**: перечитать сообщения с произвольного места
- Нужно хранить **аудит**: кто, когда, с каким результатом обработал сообщение

**Ключевые настройки:**
- `enable-auto-commit: false` — Kafka не коммитит оффсет автоматически
- `AckMode.MANUAL_IMMEDIATE` — оффсет коммитится только при вызове `acknowledge()`
- `ConsumerSeekAware` — при старте читаем оффсет из БД и сдвигаем Kafka-курсор

**Что в этой версии сделано реалистичнее:**
- В топик публикуется типизированный `OrderEvent` (заказ с позициями)
- Бизнес-данные (`case2_processed_orders`) и оффсет (`kafka_offsets`) пишутся
  в **одной транзакции** через `ManualOffsetService` — это и есть смысл кейса
- `FailureSimulator` роняет ~25% заказов: при сбое в БД не пишется ничего,
  оффсет не двигается — после рестарта сообщение перечитывается (at-least-once)
- Добавлен **audit-service** — отдельная consumer group, журнал аудита всех заказов

**Топология:**
```
Producer → [manual-offset-topic] ─┬─→ manual-offset-group → Consumer → PostgreSQL (order + offset, атомарно)
                                  └─→ case2-audit-group   → audit-service (журнал всех заказов)
```

---

## Запуск

```bash
docker-compose up -d zookeeper kafka kafka-ui postgres
docker-compose --profile case2 up -d
docker logs -f case2-consumer
```

| Сервис | Порт |
|---|---|
| case2-producer | 8083 |
| case2-audit-service | 8087 |

---

## Сценарий проверки

### Шаг 1: Отправить заказы

```bash
curl -X POST "http://localhost:8083/api/orders/random?count=10"
```

### Шаг 2: Проверить обработанные заказы и оффсеты

```bash
curl http://localhost:8083 >/dev/null   # producer
# Оффсеты и обработанные заказы — через REST API консьюмера недоступны снаружи
# напрямую, смотрим в PostgreSQL:
docker exec -it postgres psql -U kafka_user -d kafka_demo \
  -c "SELECT * FROM kafka_offsets;"
docker exec -it postgres psql -U kafka_user -d kafka_demo \
  -c "SELECT order_id, region, total_amount, source_partition, source_offset FROM case2_processed_orders;"

# Журнал аудита
curl http://localhost:8087/api/audit
```

### Шаг 3: Ручной сдвиг оффсета (Replay)

```bash
# Сдвинуть оффсет партиции 0 на 0 → после рестарта читать с offset=1
curl -X POST http://localhost:8083/api/offsets/seek \
  -H "Content-Type: application/json" \
  -d '{"partition": 0, "offset": 0}'

docker restart case2-consumer
```
Либо напрямую в БД:
```sql
UPDATE kafka_offsets SET offset_value = 0
WHERE topic = 'manual-offset-topic' AND partition_id = 0;
```

### Шаг 4: Наблюдать сбои обработки

`APP_CHAOS_FAILURE_RATE=0.25` — четверть заказов падает. В логах:
```
Processing failed for order ord-... — DB offset NOT advanced, message will be re-read on restart/seek
```
После `docker restart case2-consumer` упавшие сообщения перечитываются: `onPartitionsAssigned`
сдвигает курсор на сохранённый в БД оффсет.

---

## Тесты

```bash
cd case-2-manual-offset/consumer
mvn test
```

`ManualOffsetConsumerIntegrationTest` поднимает Embedded Kafka + H2 (вместо PostgreSQL,
без Docker) и проверяет атомарную запись заказа и оффсета.

---

## Разница в режимах AckMode

| Режим | Описание |
|---|---|
| `AUTO` | Коммит по расписанию (auto-commit-interval) |
| `BATCH` | Коммит после обработки целого batch poll() |
| `MANUAL` | `acknowledge()` при следующем poll() |
| `MANUAL_IMMEDIATE` | `acknowledge()` сразу (используется в этом кейсе) |
| `RECORD` | После каждого сообщения (автоматически) |
