-- ============================================================
-- Блок 2. Потоковый pipeline: Kafka -> stage.fact_telemetry
-- ============================================================

-- 1) База для stage-слоя
CREATE DATABASE IF NOT EXISTS stage;

-- 2) Kafka-таблица (читает топик fact_telemetry в формате JSONEachRow)
--    Брокер виден из сети compose как kafka:29092 (хосту — localhost:9092)
CREATE TABLE stage.kafka_telemetry
(
    event_id        UInt64,
    satellite_id    UInt32,
    status_id       UInt16,
    distance        Float64,
    signal_strength Float64,
    battery_level   UInt8,
    event_time      DateTime,
    updated_at      Nullable(DateTime),
    _state          Int8 COMMENT '1 - состояние, -1 - отмена состояния'
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list   = 'kafka:29092',
    kafka_topic_list    = 'fact_telemetry',
    kafka_group_name    = 'ch_stage_consumers',
    kafka_format        = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_flush_interval_ms = 1000;

-- 3) Stage-таблица (MergeTree): поля источника + виртуальные столбцы Kafka
--    (_topic, _partition, _offset, _timestamp) + служебные _dateload и _state
CREATE TABLE stage.fact_telemetry
(
    event_id        UInt64 COMMENT 'ID события телеметрии (источник)',
    satellite_id    UInt32 COMMENT 'Спутник',
    status_id       UInt16 COMMENT 'Статус',
    distance        Float64 COMMENT 'Дистанция',
    signal_strength Float64 COMMENT 'Уровень сигнала, dBm',
    battery_level   UInt8  COMMENT 'Заряд батареи, %',
    event_time      DateTime COMMENT 'Время события',
    updated_at      Nullable(DateTime) COMMENT 'Время изменения в источнике',

    -- служебные поля потоковой загрузки
    _topic     LowCardinality(String) COMMENT 'Топик Kafka',
    _partition Int32                  COMMENT 'Партиция топика',
    _offset    Int64                  COMMENT 'Смещение в партиции',
    _timestamp DateTime               COMMENT 'Timestamp сообщения Kafka',
    _dateload  DateTime  DEFAULT now() COMMENT 'Время загрузки строки',
    _state     Int8      DEFAULT 1     COMMENT '1 - состояние, -1 - отмена состояния'
)
ENGINE = MergeTree
ORDER BY (event_id, _offset)
SETTINGS index_granularity = 8192;

-- 4) Materialized View: перекладывает сообщения из Kafka-таблицы в stage
CREATE MATERIALIZED VIEW stage.mv_kafka_telemetry
TO stage.fact_telemetry
AS
SELECT
    event_id,
    satellite_id,
    status_id,
    distance,
    signal_strength,
    battery_level,
    event_time,
    updated_at,
    _topic,
    _partition,
    _offset,
    _timestamp,
    now()  AS _dateload,
    _state
FROM stage.kafka_telemetry;

-- 5) Проверка до запуска продюсера
SELECT count() AS rows_before FROM stage.fact_telemetry;
