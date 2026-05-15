-- Схема для интеграционных тестов (H2). В production используется init-db.sql + PostgreSQL.
CREATE TABLE IF NOT EXISTS processed_messages (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    message_id    VARCHAR(255) NOT NULL UNIQUE,
    topic         VARCHAR(255),
    partition_id  INTEGER,
    offset_value  BIGINT,
    payload       VARCHAR(2000),
    processed_at  TIMESTAMP
);

CREATE TABLE IF NOT EXISTS orders (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_id    VARCHAR(255) NOT NULL UNIQUE,
    product     VARCHAR(255),
    quantity    INTEGER,
    status      VARCHAR(50),
    created_at  TIMESTAMP
);
