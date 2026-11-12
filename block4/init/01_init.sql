-- Демо-схема блока 4. Выполняется автоматически при первом старте контейнера postgres.
-- Если база уже создана раньше, скрипт не выполнится: примените его вручную в psql/DBeaver.

CREATE TABLE IF NOT EXISTS public.dim_satellites (
    satellite_id serial PRIMARY KEY,
    name         text NOT NULL UNIQUE,
    norad_id     integer UNIQUE,
    operator     text,
    orbit_type   text NOT NULL DEFAULT 'LEO',
    altitude_km  numeric(8,1),
    launched_at  date,
    status       text NOT NULL DEFAULT 'active'
);

CREATE TABLE IF NOT EXISTS public.fact_telemetry (
    telemetry_id  bigserial PRIMARY KEY,
    satellite_id  integer NOT NULL REFERENCES public.dim_satellites (satellite_id),
    event_ts      timestamptz NOT NULL DEFAULT now(),
    temperature_c numeric(6,2) NOT NULL,
    battery_pct   numeric(5,2) NOT NULL,
    signal_dbm    numeric(6,2) NOT NULL,
    status        text NOT NULL DEFAULT 'ok'
);

CREATE INDEX IF NOT EXISTS fact_telemetry_satellite_id_idx ON public.fact_telemetry (satellite_id);
CREATE INDEX IF NOT EXISTS fact_telemetry_event_ts_idx     ON public.fact_telemetry (event_ts);

INSERT INTO public.dim_satellites (name, norad_id, operator, orbit_type, altitude_km, launched_at, status)
VALUES
    ('ARCTIC-1',  48201, 'Northern Sky', 'LEO',    550.0, '2022-04-12', 'active'),
    ('ARCTIC-2',  48202, 'Northern Sky', 'LEO',    551.0, '2022-04-12', 'active'),
    ('EQUATOR-1', 51110, 'EquaLink',     'GEO',  35786.0, '2023-03-01', 'active'),
    ('POLAR-7',   53007, 'PolarComm',    'SSO',    620.0, '2024-01-20', 'active')
ON CONFLICT (name) DO NOTHING;

-- Генерирует ровно n_events изменений в fact_telemetry: INSERT / UPDATE / DELETE.
-- Каждое событие меняет одну строку, поэтому число изменений = n_events.
CREATE OR REPLACE FUNCTION public.simulate_telemetry(n_events integer DEFAULT 5)
RETURNS integer
LANGUAGE plpgsql
AS $$
DECLARE
    done integer := 0;
    op   text;
    sid  integer;
    tid  bigint;
BEGIN
    IF NOT EXISTS (SELECT 1 FROM public.fact_telemetry) THEN
        SELECT satellite_id INTO sid FROM public.dim_satellites ORDER BY satellite_id LIMIT 1;
        INSERT INTO public.fact_telemetry (satellite_id, temperature_c, battery_pct, signal_dbm, status)
        VALUES (sid, 21.00, 90.00, -95.00, 'ok');
    END IF;

    WHILE done < n_events LOOP
        op := (ARRAY['insert', 'update', 'delete'])[1 + floor(random() * 3)::int];
        SELECT satellite_id INTO sid FROM public.dim_satellites ORDER BY random() LIMIT 1;

        IF op = 'insert' THEN
            INSERT INTO public.fact_telemetry (satellite_id, temperature_c, battery_pct, signal_dbm, status)
            VALUES (
                sid,
                round((15 + random() * 30)::numeric, 2),
                round((20 + random() * 80)::numeric, 2),
                round((-120 + random() * 40)::numeric, 2),
                (ARRAY['ok', 'warning', 'critical'])[1 + floor(random() * 3)::int]
            );
        ELSIF op = 'update' THEN
            SELECT telemetry_id INTO tid FROM public.fact_telemetry ORDER BY random() LIMIT 1;
            IF tid IS NOT NULL THEN
                UPDATE public.fact_telemetry
                   SET temperature_c = temperature_c + round((random() * 4 - 2)::numeric, 2),
                       battery_pct   = GREATEST(0, LEAST(100, battery_pct - round(random()::numeric, 2))),
                       signal_dbm    = signal_dbm + round((random() * 6 - 3)::numeric, 2),
                       event_ts      = now()
                 WHERE telemetry_id = tid;
            END IF;
        ELSE
            SELECT telemetry_id INTO tid FROM public.fact_telemetry ORDER BY random() LIMIT 1;
            IF tid IS NOT NULL THEN
                DELETE FROM public.fact_telemetry WHERE telemetry_id = tid;
            END IF;
        END IF;

        done := done + 1;
    END LOOP;

    RETURN n_events;
END;
$$;
