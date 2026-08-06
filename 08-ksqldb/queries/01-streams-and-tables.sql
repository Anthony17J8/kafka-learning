-- ЭТАП 8 — ЗАГОТОВКА. Запускать в ksqlDB CLI:
--   docker exec -it ksqldb ksql http://localhost:8088

SET 'auto.offset.reset' = 'earliest';

-- TODO 1: объявить STREAM поверх топика orders
-- CREATE STREAM orders_stream (
--     orderId  VARCHAR KEY,
--     userId   VARCHAR,
--     product  VARCHAR,
--     quantity INT,
--     price    DOUBLE
-- ) WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON');

-- TODO 2: оконная агрегация (аналог задачи 2 этапа 7)
-- CREATE TABLE orders_per_user_1m AS
--   SELECT userId, COUNT(*) AS cnt, SUM(quantity * price) AS revenue
--   FROM orders_stream
--   WINDOW TUMBLING (SIZE 1 MINUTE)
--   GROUP BY userId
--   EMIT CHANGES;

-- TODO 3: join двух потоков

-- Вопрос для NOTES.md: когда ksqlDB предпочтительнее нативного Kafka Streams?
