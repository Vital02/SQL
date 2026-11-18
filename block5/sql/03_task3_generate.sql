-- ============================================================
-- Задача 3. Генерация не менее 5 млн строк за текущий год (2026)
-- средствами ClickHouse
-- ============================================================
-- space_event_id      — number + 1: монотонная последовательность 1..5 000 000;
-- event_type          — arrayElement из 4 значений по rand() % 4 (равномерно);
-- severity            — 1 + rand() % 5 (равномерно, шкала 1-5);
-- started_at          — случайная секунда 2026 года (365 * 86400 = 31 536 000 с),
--                       равномерное распределение по всему году;
-- ended_at            — started_at + от 1 минуты до 3 часов.
INSERT INTO dwh.fact_space_events
SELECT
    number + 1 AS space_event_id,
    arrayElement(['solar_flare', 'geomagnetic_storm', 'radiation_belt', 'meteor_shower'],
                 1 + rand() % 4) AS event_type,
    1 + rand() % 5 AS severity,
    toDateTime('2026-01-01 00:00:00') + (rand() % 31536000) AS started_at,
    started_at + (60 + rand() % 10740) AS ended_at
FROM numbers(5000000);

-- ============ Проверки ============

-- Количество строк
SELECT count() AS total_rows FROM dwh.fact_space_events;

-- Распределение по event_type
SELECT event_type, count() AS cnt,
       round(cnt / 5000000 * 100, 2) AS pct
FROM dwh.fact_space_events
GROUP BY event_type
ORDER BY event_type;

-- Распределение по severity
SELECT severity, count() AS cnt,
       round(cnt / 5000000 * 100, 2) AS pct
FROM dwh.fact_space_events
GROUP BY severity
ORDER BY severity;

-- Число партиций и партов, размер на диске
SELECT
    uniqExact(partition) AS partitions_cnt,
    count()              AS parts_cnt,
    sum(rows)            AS rows_cnt,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk
FROM system.parts
WHERE database = 'dwh' AND table = 'fact_space_events' AND active;

-- Партиции детально
SELECT
    partition,
    count()    AS parts_cnt,
    sum(rows)  AS rows_cnt,
    formatReadableSize(sum(bytes_on_disk)) AS size_on_disk
FROM system.parts
WHERE database = 'dwh' AND table = 'fact_space_events' AND active
GROUP BY partition
ORDER BY partition;

-- Контроль: события действительно внутри 2026 года
SELECT min(started_at) AS min_started, max(started_at) AS max_started,
       min(ended_at)   AS min_ended,   max(ended_at)   AS max_ended
FROM dwh.fact_space_events;
