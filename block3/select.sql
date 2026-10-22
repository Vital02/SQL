SELECT t.event_id,
       s.name        AS satellite_name,
       st.description AS status_description,
       t.distance, t.signal_strength, t.battery_level, t.event_time
FROM fact_telemetry t
JOIN dim_satellites s ON s.satellite_id = t.satellite_id
JOIN dim_statuses  st ON st.status_id   = t.status_id
ORDER BY t.event_time;

SELECT s.name, SUM(t.distance) AS total_distance_km
FROM dim_satellites s
LEFT JOIN fact_telemetry t ON t.satellite_id = s.satellite_id
GROUP BY s.satellite_id, s.name
ORDER BY total_distance_km DESC;

SELECT s.name, t.event_time
FROM fact_telemetry t
JOIN dim_satellites s  ON s.satellite_id = t.satellite_id
JOIN dim_statuses  st  ON st.status_id   = t.status_id
WHERE st.status_name = 'lost_signal'
ORDER BY s.name, t.event_time;

SELECT o.code AS orbit_type, st.status_name, COUNT(t.event_id) AS events_cnt
FROM dim_orbit_types o
CROSS JOIN dim_statuses st
LEFT JOIN dim_satellites s ON s.orbit_type_id = o.orbit_type_id
LEFT JOIN fact_telemetry t ON t.satellite_id = s.satellite_id
                          AND t.status_id    = st.status_id
GROUP BY o.code, st.status_name
ORDER BY o.code, st.status_name;

SELECT s.name, ROUND(AVG(t.signal_strength), 2) AS avg_signal_dbm
FROM dim_satellites s
JOIN fact_telemetry t ON t.satellite_id = s.satellite_id
GROUP BY s.satellite_id, s.name
ORDER BY avg_signal_dbm ASC;
