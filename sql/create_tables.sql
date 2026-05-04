START TRANSACTION;

DROP TABLE IF EXISTS storm_events CASCADE;
DROP TABLE IF EXISTS storm_events_raw CASCADE;

CREATE TABLE storm_events_raw (
    col1 TEXT, col2 TEXT, col3 TEXT, col4 TEXT, col5 TEXT,
    col6 TEXT, col7 TEXT, col8 TEXT, col9 TEXT, col10 TEXT,
    col11 TEXT, col12 TEXT, col13 TEXT, col14 TEXT, col15 TEXT,
    col16 TEXT, col17 TEXT, col18 TEXT, col19 TEXT, col20 TEXT,
    col21 TEXT, col22 TEXT, col23 TEXT, col24 TEXT, col25 TEXT,
    col26 TEXT, col27 TEXT, col28 TEXT, col29 TEXT, col30 TEXT,
    col31 TEXT, col32 TEXT, col33 TEXT, col34 TEXT, col35 TEXT,
    col36 TEXT, col37 TEXT, col38 TEXT, col39 TEXT, col40 TEXT,
    col41 TEXT, col42 TEXT, col43 TEXT, col44 TEXT, col45 TEXT,
    col46 TEXT, col47 TEXT, col48 TEXT, col49 TEXT, col50 TEXT,
    col51 TEXT
);

CREATE TABLE storm_events (
    event_id BIGINT PRIMARY KEY,

    state TEXT NOT NULL,
    event_type TEXT NOT NULL,

    damage_property DOUBLE PRECISION,
    damage_crops DOUBLE PRECISION,

    deaths_direct INTEGER,
    deaths_indirect INTEGER,

    injuries_direct INTEGER,
    injuries_indirect INTEGER,

    begin_date TIMESTAMP,
    end_date TIMESTAMP,

    S DOUBLE PRECISION,
    severity INTEGER
);

ALTER TABLE storm_events
DROP CONSTRAINT IF EXISTS chk_dates;

ALTER TABLE storm_events
ADD CONSTRAINT chk_dates
CHECK (begin_date IS NULL OR end_date IS NULL OR begin_date <= end_date);


ALTER TABLE storm_events
DROP CONSTRAINT IF EXISTS chk_damage_positive;

ALTER TABLE storm_events
ADD CONSTRAINT chk_damage_positive
CHECK (
    damage_property >= 0 AND
    damage_crops >= 0
);


ALTER TABLE storm_events
DROP CONSTRAINT IF EXISTS chk_deaths_positive;

ALTER TABLE storm_events
ADD CONSTRAINT chk_deaths_positive
CHECK (
    deaths_direct >= 0 AND
    deaths_indirect >= 0
);


ALTER TABLE storm_events
DROP CONSTRAINT IF EXISTS chk_injuries_positive;

ALTER TABLE storm_events
ADD CONSTRAINT chk_injuries_positive
CHECK (
    injuries_direct >= 0 AND
    injuries_indirect >= 0
);

COMMIT;