# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A teaching project: six self-contained Apache Kafka scenarios ("cases") built with Java 17 + Spring Boot 3.3 + Spring Kafka. Each case is a runnable demo of one Kafka concept. The README.md (in Russian) and each `case-N-*/README.md` are the authoritative scenario guides — they document the curl commands and verification steps.

## Build & run

There is **no aggregator/parent pom and no root build** — each producer/consumer/service is an independent Maven project with its own `pom.xml`. The normal end-to-end workflow is Docker.

```bash
# 1. Start shared infrastructure (Kafka, ZooKeeper, Kafka UI, PostgreSQL)
docker-compose up -d zookeeper kafka kafka-ui postgres

# 2. Start a case (each case is a docker-compose profile: case1..case6)
docker-compose --profile case1 up -d
docker-compose --profile case1 up -d --build  # force rebuild after code changes

# 3. Stop a case
docker-compose --profile case1 down
```

Each service's `Dockerfile` is a multi-stage build that runs `mvn clean package -DskipTests` inside the image — editing Java code requires `--build` to take effect. To build/run a single module locally:

```bash
cd case-1-consumer-failover/consumer
mvn clean package        # build one module
mvn spring-boot:run      # run against localhost:9092
```

## Tests

Each case has an **Embedded Kafka integration test** in its main consumer module (`src/test/java/...`). Tests use `spring-kafka-test` `@EmbeddedKafka` — they spin up an in-process broker and need **no Docker**.

```bash
cd case-1-consumer-failover/consumer && mvn test   # run one case's tests
```

- Cases 2 and 6 use a relational DB. Their tests run on **H2 in-memory** (production uses PostgreSQL); test datasource/driver are overridden via `@SpringBootTest(properties=...)`. Case 6's consumer has no JPA — its schema for tests lives in `src/test/resources/schema.sql`.
- `-DskipTests` is still hardcoded in every `Dockerfile` on purpose — image builds stay fast; tests are run separately with `mvn test`.
- Tests are deterministic because chaos failure injection defaults to `0.0` and test properties set `app.chaos.failure-rate=0.0`.

## Architecture

Every case lives under `case-N-<name>/` and is composed of independent Maven modules:

- A **producer** (Spring Web app with REST endpoints to publish messages), one or more **consumer**/`service` apps, and an **extra downstream service** added per case (analytics / audit / reporting / aggregator / alerting / shipping).
- Java package convention: `com.kafka.demo.caseN.<role>` (e.g. `com.kafka.demo.case1.analytics`).
- Each app runs on container port `8080`; host ports are mapped distinctly in `docker-compose.yml` (8081–8096).

**Typed domain model + JSON serde**: messages are typed Java records (`OrderEvent`, `OrderLine`, `ActivityEvent`, `OrderMessage`, `BatchSummary`), serialized with Spring Kafka's `JsonSerializer`/`JsonDeserializer`. Because modules are independent and don't share a package, the producer sets `spring.json.add.type.headers=false` and each consumer pins the target type via `spring.json.value.default.type` + `spring.json.use.type.headers=false`. Each module keeps its own copy of the record (no shared module — consistent with the no-parent-pom design).

**Chaos / random failures**: every consumer module has a `chaos/FailureSimulator` that throws a `TransientProcessingException` with probability `app.chaos.failure-rate` (env `APP_CHAOS_FAILURE_RATE`, default `0.0`; docker-compose sets 0.1–0.25). Each case demonstrates a different failure path (retry, replay, DLQ, rollback).

**Kafka connectivity**: brokers advertise two listeners — `localhost:9092` for host processes, `kafka:9093` for containers on the `kafka-net` network. In-container services always use `kafka:9093`.

**Shared infrastructure** (`docker-compose.yml`): ZooKeeper-based Kafka 7.6 (Confluent), Kafka UI at http://localhost:8080, PostgreSQL 16 (db `kafka_demo`, user/pass `kafka_user`/`kafka_pass`). Schema is created at container init by `init-db.sql` (tables: `kafka_offsets`, `case2_processed_orders`, `processed_messages`, `orders`). PostgreSQL is used only by Case 2 and Case 6.

## The six cases

Each case isolates one concept; see the per-case README for verification scenarios.

| Case | Concept | Key mechanism | Extra service |
|---|---|---|---|
| 1 consumer-failover | Consumer group rebalancing | `ConsumerSeekAware`, `session.timeout.ms`, `DefaultErrorHandler` retry | analytics-service |
| 2 manual-offset | Offsets stored in PostgreSQL, replay | manual ack + order & offset persisted atomically | audit-service |
| 3 batch | Batch consumption + manual commit | `setBatchListener(true)`, `max.poll.records`, publishes `BatchSummary` | reporting-service |
| 4 partitions | Parallel reads across partitions | 3 partitions, `service` deployed as 3 instances, key routing | aggregator-service |
| 5 dlq | Dead Letter Queue | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer`, poison vs transient | alerting-service |
| 6 exactly-once | Transactional producer + idempotent consumer | `transactional.id`, `isolation.level=read_committed`, DB dedup | shipping-service |

## Conventions

- Listener/config/service classes carry detailed Russian Javadoc explaining the Kafka concept being demonstrated — keep this style when adding or modifying case code, since the project's purpose is teaching.
- Lombok is used (`@Slf4j`, `@RequiredArgsConstructor`, etc.); it is an optional dependency in each pom.
- Repositories/services for cases 2 & 6 avoid PostgreSQL-specific SQL (no `ON CONFLICT`) so the same code runs on H2 in tests — keep new DB code portable.
- When changing the set of services or ports, update `docker-compose.yml`, the per-case `README.md`, and the root `README.md` so the curl-based scenarios stay accurate. New extra services follow the same module layout (pom + Dockerfile + `CaseNXxxApplication` + `model` + `listener` + `service` + `controller` + `application.yml`).
