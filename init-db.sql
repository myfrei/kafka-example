-- =============================================================================
-- Kafka Demo Project — Database Initialization
-- =============================================================================

-- Case 2: Хранение Kafka оффсетов вручную
CREATE TABLE IF NOT EXISTS kafka_offsets (
    id           BIGSERIAL PRIMARY KEY,
    topic        VARCHAR(255)  NOT NULL,
    partition_id INTEGER       NOT NULL,
    offset_value BIGINT        NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    updated_at   TIMESTAMP     NOT NULL DEFAULT NOW(),
    UNIQUE (topic, partition_id, consumer_group)
);

-- Case 2: Бизнес-результат обработки заказов (пишется атомарно с оффсетом)
CREATE TABLE IF NOT EXISTS case2_processed_orders (
    id               BIGSERIAL PRIMARY KEY,
    order_id         VARCHAR(255)  NOT NULL UNIQUE,
    customer_id      VARCHAR(255),
    region           VARCHAR(64),
    total_amount     NUMERIC(12,2),
    source_partition INTEGER,
    source_offset    BIGINT,
    processed_at     TIMESTAMP     NOT NULL DEFAULT NOW()
);

-- Case 6: Идемпотентность — таблица обработанных сообщений
CREATE TABLE IF NOT EXISTS processed_messages (
    id             BIGSERIAL PRIMARY KEY,
    message_id     VARCHAR(255) NOT NULL UNIQUE,
    topic          VARCHAR(255) NOT NULL,
    partition_id   INTEGER      NOT NULL,
    offset_value   BIGINT       NOT NULL,
    payload        TEXT,
    processed_at   TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Case 6: Результаты бизнес-обработки (демо)
CREATE TABLE IF NOT EXISTS orders (
    id             BIGSERIAL PRIMARY KEY,
    order_id       VARCHAR(255) NOT NULL UNIQUE,
    product        VARCHAR(255),
    quantity       INTEGER,
    status         VARCHAR(50)  DEFAULT 'RECEIVED',
    created_at     TIMESTAMP    NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE kafka_offsets IS 'Case 2: Ручное управление Kafka-оффсетами';
COMMENT ON TABLE processed_messages IS 'Case 6: Идемпотентность — дедупликация сообщений';
COMMENT ON TABLE orders IS 'Case 6: Бизнес-таблица заказов';
