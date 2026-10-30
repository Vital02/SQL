import os
import random
import string
import sys
from datetime import datetime, timedelta, date

import psycopg2
from psycopg2.extras import execute_values

STEP = timedelta(minutes=5)
NEW_SATELLITE_PROB = 0.10
STATUS_WEIGHTS = {"ok": 80, "warning": 12, "failed": 5, "lost_signal": 3}
UPDATE_SHARE = (0.05, 0.15)
DELETE_SHARE = (0.02, 0.05)
RETENTION_DAYS = 90
BACKFILL_SHARE = 0.15


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

    battery = (random.randint(5, 35) if status in ("warning", "failed")
               else random.randint(30, 100))

    return (satellite_id, status_id, round(random.uniform(50, 500), 3),
            round(signal, 2), battery, event_time)


def simulate_telemetry(
    n_events: int = 100,
    *,
    new_satellite_prob: float = NEW_SATELLITE_PROB,
    update_share: tuple = UPDATE_SHARE,
    delete_share: tuple = DELETE_SHARE,
    retention_days: int = RETENTION_DAYS,
    backfill_share: float = BACKFILL_SHARE,
    seed: int | None = None,
):
    """
    n_events          — новых событий на каждый спутник за запуск.
    new_satellite_prob — вероятность добавления нового спутника.
    update_share       — диапазон доли событий для UPDATE.
    delete_share       — диапазон доли событий-кандидатов для DELETE.
    retention_days     — возраст, после которого события удаляются.
    backfill_share     — доля событий, добавляемых задним числом.
    seed               — зерно ГПСЧ для воспроизводимости.
    """
    if seed is not None:
        random.seed(seed)

    conn = get_connection()
    conn.autocommit = False
    try:
        with conn.cursor() as cur:
            cur.execute("ALTER TABLE fact_telemetry "
                        "ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT NOW()")

            cur.execute("SELECT status_id, status_name FROM dim_statuses")
            statuses = cur.fetchall()
            status_ids = [s[0] for s in statuses]
            status_names = dict(statuses)
            weights = [STATUS_WEIGHTS.get(s[1], 1) for s in statuses]

            # INSERT
            cur.execute("""SELECT s.satellite_id, MAX(t.event_time)
                             FROM dim_satellites s
                             LEFT JOIN fact_telemetry t USING (satellite_id)
                            GROUP BY s.satellite_id""")
            rows = []
            now = datetime.now()
            for satellite_id, last_time in cur.fetchall():
                current = last_time or (now - STEP * n_events)
                for i in range(n_events):
                    current += STEP + timedelta(seconds=random.randint(0, 59))
                    if last_time is not None and random.random() < backfill_share:
                        event_time = last_time - timedelta(
                            minutes=random.randint(1, n_events * 5))
                    else:
                        event_time = current
                    rows.append(random_event(status_ids, weights, status_names,
                                             event_time, satellite_id))

            if rows:
                execute_values(
                    cur,
                    """INSERT INTO fact_telemetry
                       (satellite_id, status_id, distance, signal_strength,
                        battery_level, event_time)
                       VALUES %s""",
                    rows, page_size=1000)
            inserted = len(rows)

            # UPDATE
            cur.execute("SELECT COUNT(*) FROM fact_telemetry")
            total = cur.fetchone()[0]
            updated = 0
            if total:
                k = max(1, int(total * random.uniform(*update_share)))
                cur.execute("""SELECT event_id, status_id
                                 FROM fact_telemetry
                                ORDER BY random() LIMIT %s""", (k,))
                candidates = cur.fetchall()

                ok_id = next((sid for sid, sname in status_names.items()
                              if sname == "ok"), None)

                updates = []
                for event_id, status_id in candidates:
                    if ok_id is not None and status_id != ok_id \
                            and random.random() < 0.3:
                        updates.append((
                            event_id, ok_id,
                            round(random.uniform(-100, -60), 2),
                            random.randint(60, 100)))
                    else:
                        updates.append((
                            event_id, status_id,
                            round(random.uniform(50, 500), 3),
                            random.randint(5, 100)))

                execute_values(
                    cur,
                    """UPDATE fact_telemetry AS t
                          SET status_id = v.status_id,
                              signal_strength = v.signal_strength,
                              battery_level = v.battery_level,
                              updated_at = NOW()
                         FROM (VALUES %s) AS v(event_id, status_id, signal_strength, battery_level)
                        WHERE t.event_id = v.event_id""",
                    updates,
                    template="(%s::bigint, %s::int, %s::numeric, %s::smallint)")
                updated = len(updates)

            # DELETE
            deleted = 0

            cutoff = now - timedelta(days=retention_days)
            cur.execute("DELETE FROM fact_telemetry WHERE event_time < %s", (cutoff,))
            deleted += cur.rowcount

            cur.execute("""DELETE FROM fact_telemetry
                            WHERE event_id IN (
                                SELECT event_id FROM (
                                    SELECT event_id,
                                           ROW_NUMBER() OVER (
                                               PARTITION BY satellite_id, event_time
                                               ORDER BY event_id
                                           ) AS rn
                                      FROM fact_telemetry
                                ) d
                                WHERE d.rn > 1
                            )""")
            deleted += cur.rowcount

            cur.execute("SELECT COUNT(*) FROM fact_telemetry")
            total = cur.fetchone()[0]
            if total:
                dk = max(1, int(total * random.uniform(*delete_share)))
                cur.execute("""DELETE FROM fact_telemetry
                                WHERE event_id IN (
                                    SELECT event_id FROM fact_telemetry
                                    ORDER BY random() LIMIT %s
                                )""", (dk,))
                deleted += cur.rowcount

            # Новый спутник
            added_sat = None
            if random.random() < new_satellite_prob:
                cur.execute("""SELECT orbit_type_id FROM dim_orbit_types
                                WHERE code IN ('LEO', 'MEO')""")
                orbit_ids = [r[0] for r in cur.fetchall()]
                if orbit_ids:
                    orbit_id = random.choice(orbit_ids)
                    name = "SAT-" + "".join(
                        random.choices(string.ascii_uppercase + string.digits, k=6))
                    cur.execute("""INSERT INTO dim_satellites (name, orbit_type_id, launch_date)
                                    VALUES (%s, %s, %s) RETURNING satellite_id""",
                                (name, orbit_id, date.today()))
                    added_sat = (cur.fetchone()[0], name)

        conn.commit()
        print(f"OK: вставлено {inserted}, обновлено {updated}, "
              f"удалено {deleted}, новый спутник: {added_sat}")
        return {
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "new_satellite": added_sat,
        }
    except Exception:
        conn.rollback()
        print("Ошибка, транзакция откатена", file=sys.stderr)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 100
    simulate_telemetry(n)
