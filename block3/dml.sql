INSERT INTO dim_orbit_types (code, description) VALUES
 ('LEO', 'Низкая околоземная орбита (до ~2000 км), период около 90-120 минут'),
 ('MEO', 'Средняя околоземная орбита (~2000-35786 км), период от 2 до 24 часов'),
 ('GEO', 'Геостационарная орбита (~35786 км), период равен периоду вращения Земли');

 INSERT INTO dim_statuses (status_name, description) VALUES
 ('ok', 'Спутник работает штатно'),
 ('warning', 'Обнаружено аномальное значение параметра (сигнал, заряд батареи и подобное)'),
 ('failed', 'Отказ бортовой системы, спутник не может выполнять часть функций'),
 ('lost_signal', 'С спутником потерян сигнал');

 INSERT INTO dim_satellites (name, orbit_type_id, launch_date) VALUES
 ('Kanopus-V', (SELECT orbit_type_id FROM dim_orbit_types WHERE code = 'LEO'), DATE '2018-07-22'),
 ('Meteor-M', (SELECT orbit_type_id FROM dim_orbit_types WHERE code = 'LEO'), DATE '2021-02-28'),
 ('GLONASS-K', (SELECT orbit_type_id FROM dim_orbit_types WHERE code = 'MEO'), DATE '2020-12-07'),
 ('Galileo-FOC', (SELECT orbit_type_id FROM dim_orbit_types WHERE code = 'MEO'), DATE '2019-07-25'),
 ('Express-AM', (SELECT orbit_type_id FROM dim_orbit_types WHERE code = 'GEO'), DATE '2017-03-15');

 INSERT INTO fact_telemetry (satellite_id, status_id, distance, signal_strength, battery_level, event_time)
 SELECT
     sat.satellite_id,
     st.status_id,
     round((50 + g.r_dist * 450)::numeric, 3) AS distance,
     round((CASE WHEN st.status_name = 'lost_signal' THEN -125 - g.r_sig * 15
                 WHEN st.status_name = 'warning' THEN -100 - g.r_sig * 20
                 ELSE -60 - g.r_sig * 40 END)::numeric, 2) AS signal_strength,
     (CASE WHEN st.status_name IN ('warning', 'failed')
           THEN 5 + floor(g.r_bat * 30)
           ELSE 30 + floor(g.r_bat * 71) END)::smallint AS battery_level,
     timestamp '2026-09-01 00:00:00'
         + g.n * interval '5 minutes'
         + g.r_jit * interval '4 minutes'
FROM (
    SELECT n,
           random() AS r_status, random() AS r_dist, random() AS r_sig,
           random() AS r_bat,    random() AS r_jit,
           (n % 5) + 1 AS sat_rn
    FROM generate_series(1, 12000) AS n
) g
JOIN (SELECT satellite_id, row_number() OVER (ORDER BY satellite_id) AS rn
      FROM dim_satellites) sat ON sat.rn = g.sat_rn
JOIN dim_statuses st ON st.status_name =
     CASE WHEN g.r_status < 0.80 THEN 'ok'
          WHEN g.r_status < 0.92 THEN 'warning'
          WHEN g.r_status < 0.97 THEN 'failed'
          ELSE 'lost_signal' END;
 
