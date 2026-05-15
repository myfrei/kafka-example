# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A teaching project: six self-contained Apache Kafka scenarios ("cases") built with Java 17 + Spring Boot 3.3 + Spring Kafka. Each case is a runnable demo of one Kafka concept. The README.md (in Russian) is the authoritative scenario guide — it documents the curl commands and verification steps for each case.

## Build & run

There is **no aggregator/parent pom and no root build** — each producer/consumer/service is an independent Maven project with its own `pom.xml`. The normal workflow is Docker, not local Maven.

```bash
# 1. Start shared infrastructure (Kafka, ZooKeeper, Kafka UI, PostgreSQL)
docker-compose up -d zookeeper kafka kafka-ui postgres

# 2. Start a case (each case is a docker-compose profile: case1..case6)
docker-compose --profile case1 up -d        # builds images on first run
docker-compose --profile case1 up -d --build  # force rebuild after code changes

# 3. Stop a case
docker-compose --profile case1 down
```

Each service's `Dockerfile` is a multi-stage build that runs `mvn clean package -DskipTests` inside the image — editing Java code requires `--build` to take effect. To build/run a single module locally instead:

```bash
cd case-1-consumer-failover/consumer
mvn clean package                                    # build one module
mvn spring-boot:run                                  # run against localhost:9092
```

There are **no automated tests** in this repo — `-DskipTests` is hardcoded in every Dockerfile and no `src/test` directories exist.

## Architecture

Every case follows the same shape under `case-N-<name>/`:

- A **producer** (Spring Web app exposing REST endpoints to publish messages) and one or more **consumer**/`service`/`dlq-handler` apps.
- Java package convention: `com.kafka.demo.caseN.<role>` (e.g. `com.kafka.demo.case1.consumer`).
- Each app runs on container port `8080`; producers are mapped to distinct host ports (8081–8089, see `docker-compose.yml`).
- Config lives in `src/main/resources/application.yml`; environment-specific values are injected via env vars with defaults (e.g. `${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}`).

**Kafka connectivity**: brokers advertise two listeners — `localhost:9092` for host processes, `kafka:9093` for containers on the `kafka-net` network. In-container services always use `kafka:9093`.

**Shared infrastructure** (`docker-compose.yml`): ZooKeeper-based Kafka 7.6 (Confluent), Kafka UI at http://localhost:8080, and PostgreSQL 16 (db `kafka_demo`, user/pass `kafka_user`/`kafka_pass`). Broker defaults: 3 partitions, auto-create topics on, idempotence enabled.

**PostgreSQL** is used only by Case 2 and Case 6. Schema is created at container init by `init-db.sql`: `kafka_offsets` (Case 2 manual offset storage), `processed_messages` (Case 6 dedup), `orders` (Case 6 business table).

## The six cases

Each case isolates one concept; see README.md for full verification scenarios.

| Case | Concept | Key mechanism |
|---|---|---|
| 1 consumer-failover | Consumer group rebalancing | `ConsumerSeekAware` callbacks, `session.timeout.ms` |
| 2 manual-offset | Offsets stored in PostgreSQL, replay | `enable-auto-commit: false` + JPA, `OffsetController` REST API |
| 3 batch | Batch consumption + manual commit | `setBatchListener(true)`, `max.poll.records` |
| 4 partitions | Parallel reads across partitions | 3 partitions, `service` deployed as 3 instances |
| 5 dlq | Dead Letter Queue | retry 3× then `DeadLetterPublishingRecoverer`, separate `dlq-handler` |
| 6 exactly-once | Transactional producer + idempotent consumer | `transactional.id`, `isolation.level=read_committed`, DB dedup |

## Conventions

- Listener/config classes carry detailed Russian Javadoc explaining the Kafka concept being demonstrated — keep this style when adding or modifying case code, since the project's purpose is teaching.
- Lombok is used (`@Slf4j`, etc.); it is an optional dependency in each pom.
- When changing the set of services or ports, update both `docker-compose.yml` and README.md so the curl-based scenarios stay accurate.
