#!/usr/bin/env bash
# Задача 4. Дополнительный проход: 3 прогона каждого запроса на каждом варианте,
# в отчёт берём min(query_duration_ms) — устойчивая оценка на малых объёмах.
set -uo pipefail

CH_CLIENT="docker exec -i clickhouse clickhouse-client"

q1() { echo "SELECT count() FROM $1 WHERE started_at >= toDateTime('2026-05-01 00:00:00') AND started_at < toDateTime('2026-06-01 00:00:00')"; }
q2() { echo "SELECT count() FROM $1 WHERE started_at >= toDateTime('2026-05-15 00:00:00') AND started_at < toDateTime('2026-05-16 00:00:00')"; }
q3() { echo "SELECT count() FROM $1 WHERE event_type = 'geomagnetic_storm' AND severity >= 4"; }
q4() { echo "SELECT event_type, count() AS cnt FROM $1 GROUP BY event_type ORDER BY event_type"; }

for v in A B C D; do
  case $v in
    A) T=dwh.fact_space_events_a ;;
    B) T=dwh.fact_space_events_b ;;
    C) T=dwh.fact_space_events_c ;;
    D) T=dwh.fact_space_events_d ;;
  esac
  for qn in q1 q2 q3 q4; do
    sql=$($qn "$T")
    $CH_CLIENT --query "SYSTEM DROP MARK CACHE" >/dev/null 2>&1 || true
    for r in 1 2 3; do
      $CH_CLIENT --query_id="t3_${v}_${qn}_${r}" --query "$sql" >/dev/null 2>&1
    done
  done
done
$CH_CLIENT --query "SYSTEM FLUSH LOGS" >/dev/null 2>&1

$CH_CLIENT --query "
WITH runs AS (
    SELECT
        substring(query_id, 4, 1) AS variant,
        substring(query_id, 6, 2) AS qn,
        min(query_duration_ms)    AS min_ms,
        any(read_rows)            AS read_rows,
        any(read_bytes)           AS read_bytes
    FROM system.query_log
    WHERE query_id LIKE 't3_%' AND type = 'QueryFinish'
    GROUP BY variant, qn
)
SELECT variant, qn, min_ms, read_rows, read_bytes
FROM runs
ORDER BY qn, variant
FORMAT TSV" > results/task4/timings_min3.txt
cat results/task4/timings_min3.txt
