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
- Null ключ → round-robin между партициями (равномерное распределение)

---

## Запуск

```bash
docker-compose up -d zookeeper kafka kafka-ui
docker-compose --profile case4 up -d

# Логи всех трёх инстансов
docker logs -f case4-service-1 &
docker logs -f case4-service-2 &
docker logs -f case4-service-3 &
```

---

## Сценарий проверки

### Шаг 1: Наблюдать начальное распределение партиций

При старте всех трёх инстансов:
```
[instance-1] PARTITIONS ASSIGNED: [0]
[instance-2] PARTITIONS ASSIGNED: [1]
[instance-3] PARTITIONS ASSIGNED: [2]
```

### Шаг 2: Отправить сообщения с разными ключами

```bash
# Отправляем 30 сообщений
curl "http://localhost:8085/api/messages/flood?count=30"
```

В логах увидим, что:
- instance-1 получает только сообщения из partition-0
- instance-2 — только из partition-1
- instance-3 — только из partition-2

### Шаг 3: Проверить детерминизм ключей

```bash
# Один и тот же ключ всегда попадает в одну партицию
for i in {1..5}; do
  curl -s -X POST http://localhost:8085/api/messages \
    -H "Content-Type: application/json" \
    -d '{"key": "user-42", "value": "attempt '$i'"}'
  sleep 1
done
```

Все 5 сообщений с ключом "user-42" будут прочитаны одним и тем же инстансом.

### Шаг 4: Принудительно отправить в конкретную партицию

```bash
# Отправить в partition-2 независимо от ключа
curl -X POST http://localhost:8085/api/messages/partition/2 \
  -H "Content-Type: application/json" \
  -d '{"key": "forced", "value": "this goes to partition 2"}'
```

Сообщение прочитает только instance-3 (который назначен на partition-2).

### Шаг 5: Остановить один инстанс

```bash
docker stop case4-service-2
```

Через ~10 секунд rebalancing:
```
[instance-1] PARTITIONS ASSIGNED: [0, 1]   ← взял партицию от упавшего
[instance-3] PARTITIONS ASSIGNED: [2]
```

### Шаг 6: Проверить Kafka UI

http://localhost:8080 → Topics → partitioned-topic → Partitions

Видим:
- Сколько сообщений в каждой партиции
- Какой consumer lag у каждого инстанса
- Какому инстансу назначена каждая партиция

---

## Kafka UI: Consumer Groups → partition-group → Members

```
Member ID              | Client ID    | Host       | Partitions
consumer-1-xxx         | case4-svc-1  | 172.x.x.1  | [0]
consumer-2-xxx         | case4-svc-2  | 172.x.x.2  | [1]
consumer-3-xxx         | case4-svc-3  | 172.x.x.3  | [2]
```

---

## Масштабирование

```bash
# Добавить 4-й инстанс (будет idle — нет свободных партиций)
docker run -d --name case4-service-4 \
  --network kafka-example_kafka-net \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9093 \
  -e INSTANCE_ID=instance-4 \
  kafka-example-case4-service

# В логах instance-4:
# [instance-4] PARTITIONS ASSIGNED: []  ← пустой список!
```

Чтобы задействовать 4-й инстанс — нужно увеличить число партиций:
```bash
docker exec kafka kafka-topics \
  --bootstrap-server localhost:9093 \
  --alter --topic partitioned-topic --partitions 4
```
