-- ============================================================
-- Задача 2. Создание таблицы dwh.fact_space_events
-- ============================================================

CREATE DATABASE IF NOT EXISTS dwh;

-- Обоснование выбора (кратко, подробно — в отчёте):
--   space_event_id UInt64  — положительное целое, 8 байт, монотонный уникальный id;
--   event_type     Enum8   — строка с 4 значениями => 1 байт вместо строки,
--                             встроенная валидация значений, отличное сжатие;
--   severity       UInt8   — шкала 1-5, 1 байт;
--   started_at / ended_at DateTime — дата+время до секунды, 4 байта
--                             (для космических событий секунды достаточно);
--   MergeTree      — стандартный движок OLAP-DWH в ClickHouse: первичный ключ,
--                             партиции, TTL, фоновые слияния;
--   PARTITION BY toYYYYMM(started_at) — поток событий по времени, месячные
--                             партиции: отсечение месяцев в запросах, дешёвое
--                             удаление старого (DROP PARTITION), ~12 партиций/год;
--   ORDER BY (space_event_id) — уникальный идентификатор события для точечного
--                             поиска; варианты ключей исследуются в задаче 4;
--   TTL started_at + INTERVAL 2 YEAR DELETE — ретеншн: события старше 2 лет
--                             удаляются автоматически.
CREATE TABLE IF NOT EXISTS dwh.fact_space_events
(
    space_event_id UInt64 COMMENT 'Уникальный идентификатор события',
    event_type     Enum8('solar_flare' = 1,
                         'geomagnetic_storm' = 2,
                         'radiation_belt' = 3,
                         'meteor_shower' = 4) COMMENT 'Тип события',
    severity       UInt8 COMMENT 'Сила события, шкала 1-5',
    started_at     DateTime COMMENT 'Начало события',
    ended_at       DateTime COMMENT 'Окончание события',
    -- вторичный индекс: помогает отсекать гранулы по времени,
    -- когда started_at не входит в первичный ключ
    INDEX idx_started_at started_at TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(started_at)
ORDER BY (space_event_id)
TTL started_at + INTERVAL 2 YEAR DELETE
SETTINGS index_granularity = 8192;

-- Проверка DDL
SHOW CREATE TABLE dwh.fact_space_events;

-- Ручные вставки (3 отдельных INSERT -> 3 отдельных парта)
INSERT INTO dwh.fact_space_events VALUES
    (1, 'solar_flare', 4, toDateTime('2026-03-14 11:20:00'), toDateTime('2026-03-14 14:05:00'));

INSERT INTO dwh.fact_space_events VALUES
    (2, 'geomagnetic_storm', 5, toDateTime('2026-05-02 23:40:00'), toDateTime('2026-05-03 10:15:00'));

INSERT INTO dwh.fact_space_events VALUES
    (3, 'meteor_shower', 2, toDateTime('2026-08-12 02:00:00'), toDateTime('2026-08-12 06:30:00')),
    (4, 'radiation_belt', 3, toDateTime('2026-10-27 17:55:00'), toDateTime('2026-10-27 21:10:00'));

-- Просмотр вставленных данных
SELECT * FROM dwh.fact_space_events ORDER BY space_event_id;

-- system.parts: каждый INSERT создал свой парт
SELECT
    partition,
    name,
    rows     AS rows_cnt,
    formatReadableSize(bytes_on_disk) AS size_on_disk
FROM system.parts
WHERE database = 'dwh' AND table = 'fact_space_events' AND active
ORDER BY partition, name;
