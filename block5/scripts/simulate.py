"""
Практика 2026-11-16. Блок 2. Адаптация Python-скрипта simulate из практики block3.

Источник событий телеметрии fact_telemetry: вместо INSERT/UPDATE в PostgreSQL
скрипт публикует сообщения в Kafka-топик `fact_telemetry` в формате JSONEachRow,
который читается таблицей stage.kafka_telemetry (ENGINE = Kafka) в ClickHouse.

Логика взята из scripts практики block3 (simulate_telemetry.py):
  * те же веса статусов ok/warning/failed/lost_signal и правила генерации
    signal_strength / battery_level / distance;
  * INSERT  — новые события, публикуются как новая строка с _state = 1;
  * UPDATE  — изменения случайных ранее вставленных событий; в stage
    отправляются ДВЕ строки: старое значение с _state = -1 (отмена
    предыдущего состояния) и новое значение с _state = 1 (новое состояние).
    Stage-таблица — append-only, история состояний хранится строками.

Запуск:
    pip install kafka-python
    python simulate.py [n_events]          # по умолчанию 100 на спутник

Переменные окружения:
    KAFKA_BOOTSTRAP  (по умолчанию localhost:9092)
    TOPIC            (по умолчанию fact_telemetry)
    N_SATELLITES     (по умолчанию 10)
"""

import json
import os
import random
import sys
import time
from datetime import datetime, timedelta

from kafka import KafkaProducer

STEP = timedelta(minutes=5)
STATUS_WEIGHTS = [80, 12, 5, 3]  # ok, warning, failed, lost_signal (id 1..4)
STATUS_SIGNAL_RANGES = {
    "lost_signal": (-140, -125),
    "warning": (-120, -100),
}
OK_SIGNAL_RANGE = (-100, -60)
UPDATE_SHARE = (0.05, 0.15)
N_SATELLITES = int(os.getenv("N_SATELLITES", "10"))
TOPIC = os.getenv("TOPIC", "fact_telemetry")


def random_event(status_id, event_time, satellite_id):
    status = {1: "ok", 2: "warning", 3: "failed", 4: "lost_signal"}[status_id]

    if status in STATUS_SIGNAL_RANGES:
        lo, hi = STATUS_SIGNAL_RANGES[status]
        signal = random.uniform(lo, hi)
    else:
        signal = random.uniform(*OK_SIGNAL_RANGE)

    battery = (random.randint(5, 35) if status in ("warning", "failed")
               else random.randint(30, 100))

    return {
        "satellite_id": satellite_id,
        "status_id": status_id,
        "distance": round(random.uniform(50, 500), 3),
        "signal_strength": round(signal, 2),
        "battery_level": battery,
        "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
    }


def pick_status():
    return random.choices([1, 2, 3, 4], weights=STATUS_WEIGHTS, k=1)[0]


def main(n_events=100):
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda v: str(v).encode("utf-8"),
        acks=1,
        linger_ms=100,
    )

    # Уникальные event_id между запусками: база = миллисекунды старта
    id_base = int(time.time() * 1000) * 1000
    next_id = 0
    inserted_ids = {}  # event_id -> последняя версия строки

    now = datetime.now()
    rows = []

    # ---------- INSERT ----------
    for satellite_id in range(1, N_SATELLITES + 1):
        current = now - STEP * n_events
        for _ in range(n_events):
            current += STEP + timedelta(seconds=random.randint(0, 59))
            event = random_event(pick_status(), current, satellite_id)
            event["event_id"] = id_base + next_id
            next_id += 1
            event["updated_at"] = None
            event["_state"] = 1
            rows.append(event)
            inserted_ids[event["event_id"]] = event

    for row in rows:
        producer.send(TOPIC, key=row["event_id"], value=row)

    # ---------- UPDATE ----------
    updated = 0
    if inserted_ids:
        k = max(1, int(len(inserted_ids) * random.uniform(*UPDATE_SHARE)))
        candidates = random.sample(list(inserted_ids), min(k, len(inserted_ids)))
        for event_id in candidates:
            old = inserted_ids[event_id]
            # отмена предыдущего состояния строки
            producer.send(TOPIC, key=event_id, value={**old, "_state": -1})
            # новое состояние строки
            new = dict(old)
            new["status_id"] = pick_status()
            new["distance"] = round(random.uniform(50, 500), 3)
            new["signal_strength"] = round(random.uniform(-100, -60), 2)
            new["battery_level"] = random.randint(5, 100)
            new["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new["_state"] = 1
            producer.send(TOPIC, key=new["event_id"], value=new)
            inserted_ids[event_id] = new
            updated += 1

    producer.flush()
    producer.close()
    print(f"OK: отправлено в Kafka '{TOPIC}': вставок {len(rows)}, "
          f"обновлений {updated} (по 2 сообщения: _state=-1 и _state=1), "
          f"всего сообщений {len(rows) + updated * 2}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    main(n)
