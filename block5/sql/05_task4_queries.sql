-- ============================================================
-- Задача 4. Четыре тестовых запроса (шаблоны: {T} = имя таблицы)
-- Замеры: EXPLAIN indexes = 1 + system.query_log (query_id)
-- ============================================================

-- Q1. События за один конкретный месяц (май 2026)
-- SELECT count() FROM {T}
--  WHERE started_at >= toDateTime('2026-05-01 00:00:00')
--    AND started_at <  toDateTime('2026-06-01 00:00:00');

-- Q2. События за один конкретный день (15 мая 2026)
-- SELECT count() FROM {T}
--  WHERE started_at >= toDateTime('2026-05-15 00:00:00')
--    AND started_at <  toDateTime('2026-05-16 00:00:00');

-- Q3. Геомагнитные бури с severity >= 4
-- SELECT count() FROM {T}
--  WHERE event_type = 'geomagnetic_storm' AND severity >= 4;

-- Q4. Количество событий по event_type за год
-- SELECT event_type, count() AS cnt
--   FROM {T}
--  GROUP BY event_type
--  ORDER BY event_type;

-- Пример замера с фиксированным query_id (далее выполняется из скрипта runner'а):
--   clickhouse-client --query_id="q1_a" --query "SELECT count() FROM dwh.fact_space_events_a WHERE ..."
--   SYSTEM FLUSH LOGS;
--   SELECT query_duration_ms, read_rows, read_bytes
--     FROM system.query_log
--    WHERE query_id = 'q1_a' AND type = 'QueryFinish';

-- Пример EXPLAIN для варианта A, запрос Q1:
-- EXPLAIN indexes = 1
-- SELECT count() FROM dwh.fact_space_events_a
--  WHERE started_at >= toDateTime('2026-05-01 00:00:00')
--    AND started_at <  toDateTime('2026-06-01 00:00:00');
