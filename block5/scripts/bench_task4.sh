#!/usr/bin/env bash
# =============================================================
# Задача 4. Прогон 4 тестовых запросов на вариантах A/B/C.
# Для каждой пары (вариант, запрос):
#   * EXPLAIN indexes = 1  -> отсечение партов и гранул (Parts: x/y, Granules: x/y)
#   * холодный прогон (после SYSTEM DROP MARK CACHE / UNMARKED CACHE) с query_id
#   * тёплый прогон с query_id
#   * тайминги из system.query_log: query_duration_ms, read_rows, read_bytes
# Результаты: results/task4/variant_{A,B,C}.txt
# =============================================================
set -uo pipefail

CH_CLIENT="docker exec -i clickhouse clickhouse-client"
OUT_DIR="results/task4"
mkdir -p "$OUT_DIR"

q1() { echo "SELECT count() FROM $1 WHERE started_at >= toDateTime('2026-05-01 00:00:00') AND started_at < toDateTime('2026-06-01 00:00:00')"; }
q2() { echo "SELECT count() FROM $1 WHERE started_at >= toDateTime('2026-05-15 00:00:00') AND started_at < toDateTime('2026-05-16 00:00:00')"; }
q3() { echo "SELECT count() FROM $1 WHERE event_type = 'geomagnetic_storm' AND severity >= 4"; }
q4() { echo "SELECT event_type, count() AS cnt FROM $1 GROUP BY event_type ORDER BY event_type"; }

for v in ${1:-"A B C"}; do
  case $v in
    A) T=dwh.fact_space_events_a ;;
    B) T=dwh.fact_space_events_b ;;
    C) T=dwh.fact_space_events_c ;;
    D) T=dwh.fact_space_events_d ;;
  esac
  out="$OUT_DIR/variant_${v}.txt"
  : > "$out"
  for qn in q1 q2 q3 q4; do
    sql=$($qn "$T")
    {
      echo "==============================================================="
      echo "Вариант $v, запрос $qn"
      echo "SQL: $sql"
      echo "---------------------------------------------------------------"
      echo "EXPLAIN indexes = 1:"
    } >> "$out"
    $CH_CLIENT --query "EXPLAIN indexes = 1 $sql" >> "$out" 2>&1
    # холодный прогон: сброс кешей меток
    $CH_CLIENT --query "SYSTEM DROP MARK CACHE" >/dev/null 2>&1 || true
    $CH_CLIENT --query "SYSTEM DROP UNMARKED CACHE" >/dev/null 2>&1 || true
    cid="task4_${v}_${qn}_cold"
    $CH_CLIENT --query_id="$cid" --query "$sql" >> "$out" 2>&1
    # тёплый прогон
    wid="task4_${v}_${qn}_warm"
    $CH_CLIENT --query_id="$wid" --query "$sql" >> "$out" 2>&1
    $CH_CLIENT --query "SYSTEM FLUSH LOGS" >/dev/null 2>&1
    {
      echo "---------------------------------------------------------------"
      echo "system.query_log (query_duration_ms | read_rows | read_bytes):"
    } >> "$out"
    $CH_CLIENT --query "
      SELECT query_id, query_duration_ms, read_rows, read_bytes
      FROM system.query_log
      WHERE query_id IN ('$cid', '$wid') AND type = 'QueryFinish'
      ORDER BY query_id
      FORMAT PrettyCompact" >> "$out" 2>&1
    echo "" >> "$out"
  done
done
echo "benchmark done"
