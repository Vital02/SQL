-- Блок 4. CDC + Kafka + Debezium
-- Подключение: host=localhost, port=5432, user=postgres, password=postgres, database=main


-- ============================ БЛОК 1. CDC ============================

-- Задание 1. Подготовка WAL

SHOW wal_level;

SELECT pg_current_wal_lsn()                  AS current_lsn,
       pg_walfile_name(pg_current_wal_lsn()) AS wal_file;

-- shell: docker exec -it postgres /usr/lib/postgresql/16/bin/pg_waldump \
--          /var/lib/postgresql/data/pg_wal/<wal_file>


-- Задание 2. Слот и публикация

-- test_decoding
SELECT pg_create_logical_replication_slot('slot_td', 'test_decoding');

SELECT simulate_telemetry(10);

SELECT * FROM pg_logical_slot_peek_changes('slot_td', NULL, NULL);

CREATE TEMP TABLE td_changes AS
SELECT * FROM pg_logical_slot_get_changes('slot_td', NULL, NULL);

SELECT data FROM td_changes;

SELECT (regexp_match(data, '^table \S+: (INSERT|UPDATE|DELETE):'))[1] AS op,
       count(*) AS cnt
  FROM td_changes
 WHERE data LIKE 'table %'
 GROUP BY 1
 ORDER BY 1;

SELECT pg_drop_replication_slot('slot_td');

-- pgoutput + publication
SELECT pg_create_logical_replication_slot('slot_pgo', 'pgoutput');

CREATE PUBLICATION pub_blk1
    FOR TABLE public.fact_telemetry, public.dim_satellites;

SELECT * FROM pg_publication_tables WHERE pubname = 'pub_blk1';

SELECT simulate_telemetry(10);

INSERT INTO public.dim_satellites (name, norad_id, operator, orbit_type, altitude_km, launched_at, status)
VALUES ('BLK1-TEST-' || to_char(clock_timestamp(), 'HH24MISS'),
        (40000 + floor(random() * 20000))::int,
        'Test Operator', 'LEO', 600.0, current_date, 'active');

UPDATE public.dim_satellites
   SET status = 'testing'
 WHERE name LIKE 'BLK1-TEST-%';

SELECT lsn, xid, encode(data, 'escape') AS data
  FROM pg_logical_slot_peek_binary_changes(
        'slot_pgo', NULL, NULL,
        'proto_version', '1',
        'publication_names', 'pub_blk1');


-- Задание 3. before / after: DEFAULT vs FULL

SELECT pg_create_logical_replication_slot('slot_td', 'test_decoding');

SELECT relreplident FROM pg_class WHERE relname = 'fact_telemetry';

-- DEFAULT: только new-tuple
UPDATE public.fact_telemetry
   SET temperature_c = temperature_c + 1.5
 WHERE telemetry_id IN (SELECT telemetry_id FROM public.fact_telemetry ORDER BY telemetry_id LIMIT 3);

SELECT data FROM pg_logical_slot_get_changes('slot_td', NULL, NULL);

-- DELETE при DEFAULT: old-key только с PK
DELETE FROM public.fact_telemetry
 WHERE telemetry_id = (SELECT max(telemetry_id) FROM public.fact_telemetry);

SELECT data FROM pg_logical_slot_get_changes('slot_td', NULL, NULL);

-- FULL: old-key + new-tuple
ALTER TABLE public.fact_telemetry REPLICA IDENTITY FULL;

UPDATE public.fact_telemetry
   SET temperature_c = temperature_c + 1.5
 WHERE telemetry_id IN (SELECT telemetry_id FROM public.fact_telemetry ORDER BY telemetry_id LIMIT 3);

-- дельта: python diff_update.py update_full.txt
SELECT data FROM pg_logical_slot_get_changes('slot_td', NULL, NULL);

-- DELETE при FULL: old-key со всей строкой
DELETE FROM public.fact_telemetry
 WHERE telemetry_id = (SELECT max(telemetry_id) FROM public.fact_telemetry);

SELECT data FROM pg_logical_slot_get_changes('slot_td', NULL, NULL);


-- Чистка перед блоком 2 (debezium_slot не трогаем)

SELECT pg_drop_replication_slot(slot_name)
  FROM pg_replication_slots
 WHERE slot_name IN ('slot_td', 'slot_pgo');

DROP PUBLICATION IF EXISTS pub_blk1;


-- ==================== БЛОК 2. Kafka + Debezium ====================

-- before в событии возможен только при REPLICA IDENTITY FULL
ALTER TABLE public.fact_telemetry REPLICA IDENTITY FULL;
ALTER TABLE public.dim_satellites  REPLICA IDENTITY FULL;

-- Debezium создаёт слот и публикацию сам при старте коннектора
SELECT slot_name, plugin, database, active, confirmed_flush_lsn
  FROM pg_replication_slots
 WHERE slot_name = 'debezium_slot';

SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_pub';

SELECT simulate_telemetry(10);

-- INSERT + UPDATE в fact_telemetry (топик nrt.public.fact_telemetry)
INSERT INTO public.fact_telemetry (satellite_id, temperature_c, battery_pct, signal_dbm, status)
VALUES (1, 21.50, 88.00, -95.50, 'blk2-insert');

UPDATE public.fact_telemetry
   SET temperature_c = 25.75,
       battery_pct   = 84.00
 WHERE status = 'blk2-insert';

-- INSERT + UPDATE в dim_satellites (топик nrt.public.dim_satellites)
INSERT INTO public.dim_satellites (name, norad_id, operator, orbit_type, altitude_km, launched_at, status)
VALUES ('BLK2-DIM', 60001, 'Test Operator', 'LEO', 610.0, current_date, 'active')
ON CONFLICT (name) DO NOTHING;

UPDATE public.dim_satellites
   SET altitude_km = 615.0,
       status      = 'testing'
 WHERE name = 'BLK2-DIM';
