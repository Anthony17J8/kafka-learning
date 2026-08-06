-- Инициализация БД для этапов 6 и 11 (Debezium CDC + Outbox).
-- Файл автоматически выполняется при первом старте контейнера postgres.

CREATE SCHEMA IF NOT EXISTS shop;

CREATE TABLE IF NOT EXISTS shop.customers (
    id          SERIAL PRIMARY KEY,
    email       TEXT NOT NULL UNIQUE,
    full_name   TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS shop.orders (
    id          SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES shop.customers(id),
    product     TEXT NOT NULL,
    quantity    INT NOT NULL CHECK (quantity > 0),
    price       NUMERIC(12,2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'CREATED',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ЭТАП 11: таблица outbox. Пишется в ТОЙ ЖЕ транзакции, что и бизнес-данные.
CREATE TABLE IF NOT EXISTS shop.outbox (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type TEXT NOT NULL,   -- -> имя топика (Outbox Event Router SMT)
    aggregate_id   TEXT NOT NULL,   -- -> ключ сообщения
    event_type     TEXT NOT NULL,   -- -> заголовок сообщения
    payload        JSONB NOT NULL,  -- -> тело сообщения
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- REPLICA IDENTITY FULL нужна, чтобы Debezium видел "before"-образ строки
ALTER TABLE shop.orders REPLICA IDENTITY FULL;
ALTER TABLE shop.customers REPLICA IDENTITY FULL;

INSERT INTO shop.customers (email, full_name) VALUES
    ('anna@example.com',  'Anna Ivanova'),
    ('boris@example.com', 'Boris Petrov'),
    ('vera@example.com',  'Vera Sidorova')
ON CONFLICT DO NOTHING;
