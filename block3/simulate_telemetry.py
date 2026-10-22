import os
import random
import string
import sys
from datetime import datetime, timedelta, date

import psycopg2
from psycopg2.extras import execute_values

NEW_SATELLITE_PROB = 0.10
UPDATE_SHARE = (0.01, 0.02)
STATUS_WEIGHTS = {"ok": 80, "warning": 12, "failed": 5, "lost_signal": 3}
STEP = timedelta(minutes=5)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=os.getenv("PGPORT", "4444"),
        dbname=os.getenv("PGDATABASE", "database"),
        user=os.getenv("PGUSER", "admin"),
        password=os.getenv("PGPASSWORD", "4444"),
    )


def random_event(status_ids, weights, status_names, event_time, satellite_id):
    status_id = random.choices(status_ids, weights=weights, k=1)[0]
    status = status_names[status_id]
    if status == "lost_signal":
        signal = random.uniform(-140, -125)
    elif status == "warning":
        signal = random.uniform(-120, -100)
    else:
        signal = random.uniform(-100, -60)
    battery = random.randint(5, 35) if status in ("warning", "failed") else random.randint(30, 100)
    return (satellite_id, status_id, round(random.uniform(50, 500), 3),
            round(signal, 2), battery, event_time)


def simulate_telemetry(n_events):
    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            # колонка для инкрементальной загрузки в DWH
            cur.execute("ALTER TABLE fact_telemetry "
                        "ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")

            # 2. справочники
            cur.execute("SELECT status_id, status_name FROM dim_statuses")
            statuses = cur.fetchall()
            status_ids = [s[0] for s in statuses]
            status_names = dict(statuses)
            weights = [STATUS_WEIGHTS.get(s[1], 1) for s in statuses]

            # 4. UPDATE старых событий
            cur.execute("SELECT COUNT(*) FROM fact_telemetry")
            total = cur.fetchone()[0]
            updated = 0
            if total:
                k = max(1, int(total * random.uniform(*UPDATE_SHARE)))
                cur.execute("SELECT event_id FROM fact_telemetry ORDER BY random() LIMIT %s", (k,))
                shifts = [(eid, random.choice([-1, 1]) * random.randint(1, 300))
                          for (eid,) in cur.fetchall()]
                execute_values(
                    cur,
                    """UPDATE fact_telemetry AS t
                          SET event_time = t.event_time + v.shift * interval '1 second',
                              updated_at = NOW()
                         FROM (VALUES %s) AS v(event_id, shift)
                        WHERE t.event_id = v.event_id""",
                    shifts, template="(%s::bigint, %s::int)")
                updated = len(shifts)

            # 3a. новый спутник
            added_sat = None
            if random.random() < NEW_SATELLITE_PROB:
                cur.execute("SELECT orbit_type_id FROM dim_orbit_types")
                orbit_id = random.choice([r[0] for r in cur.fetchall()])
                name = "SAT-" + "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
                cur.execute("INSERT INTO dim_satellites (name, orbit_type_id, launch_date) "
                            "VALUES (%s, %s, %s) RETURNING satellite_id",
                            (name, orbit_id, date.today()))
                added_sat = (cur.fetchone()[0], name)

            # 3b. события для каждого спутника
            cur.execute("""SELECT s.satellite_id, MAX(t.event_time)
                             FROM dim_satellites s
                             LEFT JOIN fact_telemetry t USING (satellite_id)
                            GROUP BY s.satellite_id""")
            rows = []
            for satellite_id, last_time in cur.fetchall():
                current = (last_time or datetime.now() - STEP * n_events)
                for _ in range(n_events):
                    current += STEP + timedelta(seconds=random.randint(0, 59))
                    rows.append(random_event(status_ids, weights, status_names,
                                             current, satellite_id))

            execute_values(
                cur,
                """INSERT INTO fact_telemetry
                   (satellite_id, status_id, distance, signal_strength, battery_level, event_time)
                   VALUES %s""",
                rows, page_size=1000)

        conn.commit()   # 5. единая транзакция
        print(f"OK: вставлено {len(rows)} событий, обновлено {updated}, "
              f"новый спутник: {added_sat}")
    except Exception:
        conn.rollback()
        print("Ошибка, транзакция откатена", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    simulate_telemetry(int(sys.argv[1]) if len(sys.argv) > 1 else 100)
