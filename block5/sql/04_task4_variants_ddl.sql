-- ============================================================
-- Задача 4. Тестирование ключа партиционирования и ключа сортировки
-- Три варианта таблиц с одинаковой структурой и одинаковыми данными
-- ============================================================

-- ---------- Вариант A: без партиционирования, ORDER BY space_event_id ----------
CREATE TABLE dwh.fact_space_events_a
(
    space_event_id UInt64,
    event_type     Enum8('solar_flare' = 1, 'geomagnetic_storm' = 2,
                         'radiation_belt' = 3, 'meteor_shower' = 4),
    severity       UInt8,
    started_at     DateTime,
    ended_at       DateTime
)
ENGINE = MergeTree
ORDER BY (space_event_id)
TTL started_at + INTERVAL 2 YEAR DELETE
SETTINGS index_granularity = 8192;

-- ---------- Вариант B: PARTITION BY toYYYYMM, ORDER BY started_at ----------
CREATE TABLE dwh.fact_space_events_b
(
    space_event_id UInt64,
    event_type     Enum8('solar_flare' = 1, 'geomagnetic_storm' = 2,
                         'radiation_belt' = 3, 'meteor_shower' = 4),
    severity       UInt8,
    started_at     DateTime,
    ended_at       DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (started_at)
TTL started_at + INTERVAL 2 YEAR DELETE
SETTINGS index_granularity = 8192;

-- ---------- Вариант C: PARTITION BY toDate, ORDER BY event_type, severity, started_at ----------
CREATE TABLE dwh.fact_space_events_c
(
    space_event_id UInt64,
    event_type     Enum8('solar_flare' = 1, 'geomagnetic_storm' = 2,
                         'radiation_belt' = 3, 'meteor_shower' = 4),
    severity       UInt8,
    started_at     DateTime,
    ended_at       DateTime
)
ENGINE = MergeTree
PARTITION BY toDate(started_at)
ORDER BY (event_type, severity, started_at)
TTL started_at + INTERVAL 2 YEAR DELETE
SETTINGS index_granularity = 8192;

-- ---------- Загрузка одинаковых данных (INSERT ... SELECT) ----------
INSERT INTO dwh.fact_space_events_a
SELECT space_event_id, event_type, severity, started_at, ended_at FROM dwh.fact_space_events;

INSERT INTO dwh.fact_space_events_b
SELECT space_event_id, event_type, severity, started_at, ended_at FROM dwh.fact_space_events;

INSERT INTO dwh.fact_space_events_c
SELECT space_event_id, event_type, severity, started_at, ended_at FROM dwh.fact_space_events
-- 365 дневных партиций в одном INSERT превышает дефолтный лимит
-- max_partitions_per_insert_block = 100 (ClickHouse предупреждает против
-- избыточного партиционирования) — на время загрузки повышаем лимит:
SETTINGS max_partitions_per_insert_block = 400;

-- ---------- Контроль заполнения ----------
SELECT 'a' AS variant, count() AS cnt, min(space_event_id) AS min_id, max(space_event_id) AS max_id FROM dwh.fact_space_events_a
UNION ALL
SELECT 'b', count(), min(space_event_id), max(space_event_id) FROM dwh.fact_space_events_b
UNION ALL
SELECT 'c', count(), min(space_event_id), max(space_event_id) FROM dwh.fact_space_events_c;
