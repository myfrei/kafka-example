# Case 4 — 3 Partitions × 3 Service Instances

## Описание кейса

Демонстрирует параллельное чтение из топика с 3 партициями тремя инстансами одного сервиса.

**Правила распределения партиций:**
- Один консьюмер в группе может читать несколько партиций
- Одну партицию читает только один консьюмер в группе (гарантия порядка)
- Если инстансов > партиций → лишние инстансы простаивают
- При падении инстанса его партиции уходят к оставшимся

**Роль ключа сообщения:**
```
partition = murmur2(key.getBytes()) % numPartitions
```
- Одинаковый ключ → всегда одна партиция → гарантия порядка для этого ключа

**Что в этой версии сделано реалистичнее:**
- В топик публикуется типизированный `ActivityEvent` (активность покупателя),
  ключ — `customerId`: все события одного покупателя идут в одну партицию по порядку
- `FailureSimulator` роняет ~10% событий
- Добавлен **aggregator-service** — один инстанс в своей группе, видит ВСЕ партиции
  и строит общую агрегацию (контраст к 3 инстансам, делящим партиции)

**Топология:**
```
                                  ┌─→ partition-group (3 инстанса) → каждый по 1 партиции
Producer → [partitioned-topic] ───┤
           (3 партиции, key=cust) └─→ activity-aggregator-group (1 инстанс) → все партиции
```

---

## Запуск

```bash
docker-compose up -d zookeeper kafka kafka-ui
docker-compose --profile case4 up -d

docker logs -f case4-service-1 &
docker logs -f case4-service-2 &
docker logs -f case4-service-3 &
```

| Сервис | Порт |
|---|---|
| case4-producer | 8085 |
| case4-aggregator-service | 8091 |

---

## Сценарий проверки

### Шаг 1: Начальное распределение партиций

```
[instance-1] PARTITIONS ASSIGNED: [0]
[instance-2] PARTITIONS ASSIGNED: [1]
[instance-3] PARTITIONS ASSIGNED: [2]
```

### Шаг 2: Отправить события

```bash
curl "http://localhost:8085/api/messages/flood?count=30"
```
В логах: каждый инстанс получает события только своей партиции.

### Шаг 3: Детерминизм ключа

```bash
for i in {1..5}; do
  curl -s -X POST "http://localhost:8085/api/messages?customerId=customer-42"
done
```
Все 5 событий `customer-42` прочитает один и тот же инстанс.

### Шаг 4: Принудительная партиция

```bash
curl -X POST "http://localhost:8085/api/messages/partition/2?customerId=anyone"
```
Прочитает только инстанс, которому назначена partition-2.

### Шаг 5: Остановить инстанс

```bash
docker stop case4-service-2
```
Через ~10с rebalancing — partition-1 уходит к другому инстансу.

### Шаг 6: Сравнить со срезом aggregator-service

```bash
# partition-group: статистика конкретного инстанса (только его партиции)
# aggregator: полная картина по всем партициям сразу
curl http://localhost:8091/api/aggregate
```

---

## Тесты

```bash
cd case-4-partitions/service
mvn test
```

`PartitionConsumerIntegrationTest` поднимает Embedded Kafka с 3 партициями,
шлёт события с 30 разными ключами и проверяет, что они разложились по партициям.

---

## Масштабирование

4-й инстанс в `partition-group` будет idle (нет свободных партиций). Чтобы его
задействовать — увеличить число партиций:
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9093 \
  --alter --topic partitioned-topic --partitions 4
```
