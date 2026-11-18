# Практика «ClickHouse» (2026-11-16). Блок 1 — таблицы и партиционирование, Блок 2 — kafka-consumer

## Состав

- `docker-compose.yaml` — ClickHouse (single node) + Kafka
- `sql/` — все DDL/DML по задачам, в порядке выполнения
- `scripts/simulate.py` — продюсер телеметрии (вставки и UPDATE) в Kafka
- `scripts/bench_task4.sh`, `scripts/timings3_task4.sh` — замеры Задачи 4
- `report.md` — отчёт с обоснованиями и выводами
- `screenshots/` — скриншоты результатов запросов
- `results/` — текстовые выводы всех запросов

## Запуск

```bash
docker compose up -d                 # clickhouse (8123/9000) + kafka (9092)
```

DBeaver: `jdbc:clickhouse://localhost:8123`, пользователь `default`, без пароля.

### Блок 1

```bash
docker exec -i clickhouse clickhouse-client --multiquery < sql/01_task1_env_check.sql
docker exec -i clickhouse clickhouse-client --multiquery < sql/02_task2_ddl.sql
docker exec -i clickhouse clickhouse-client --multiquery < sql/03_task3_generate.sql
docker exec -i clickhouse clickhouse-client --multiquery < sql/04_task4_variants_ddl.sql
#  (в 04 INSERT в вариант C выполняется отдельно с SETTINGS max_partitions_per_insert_block = 400)
bash scripts/bench_task4.sh          # EXPLAIN indexes=1 + query_log для A/B/C (D: bash scripts/bench_task4.sh D)
bash scripts/timings3_task4.sh       # min время из 3 прогонов
```

### Блок 2

```bash
# 1. топик
docker exec kafka-block5 kafka-topics --bootstrap-server localhost:9092 \
  --create --topic fact_telemetry --partitions 1 --replication-factor 1

# 2. pipeline (Kafka-таблица, stage-таблица, MV)
docker exec -i clickhouse clickhouse-client --multiquery < sql/06_block2_kafka_pipeline.sql

# 3. количество строк до (ожидаемо 0)
docker exec clickhouse clickhouse-client --query "SELECT count() FROM stage.fact_telemetry"

# 4. продюсер (вставки и UPDATE)
pip install kafka-python
python scripts/simulate.py 100

# 5. количество строк после — растёт (~1-2 сек на конвейер)
docker exec clickhouse clickhouse-client --query "SELECT count() FROM stage.fact_telemetry"
```

Логика `simulate.py`: INSERT отправляет строку с `_state=1`; UPDATE — две строки:
`_state=-1` (отмена предыдущего состояния) и `_state=1` (новая версия с заполненным `updated_at`).
Stage-таблица append-only: история состояний хранится строками, актуальная версия — последняя по `_offset`.
