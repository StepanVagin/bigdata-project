"""ETL pipeline for Storm Events PostgreSQL database."""
import glob
from pprint import pprint
import psycopg2 as psql

conn = psql.connect(
    dbname="team29_projectdb",
    user="team29",
    password=open("secrets/.psql.pass", encoding="utf-8").read().strip(),
    host="hadoop-04.uni.innopolis.ru"
)

conn.autocommit = True
cur = conn.cursor()

with open("sql/create_tables.sql", encoding="utf-8") as f:
    cur.execute(f.read())

with open("sql/import_data.sql", encoding="utf-8") as f:
    copy_sql = f.readline().strip()

csv_files = sorted(glob.glob("data/StormEvents_details*.csv"))

for file in csv_files:
    print("Loading", file)
    with open(file, "r", encoding="utf-8") as f:
        cur.copy_expert(copy_sql, f)


cur.execute("""
INSERT INTO storm_events (
    event_id,
    state,
    event_type,
    damage_property,
    damage_crops,
    deaths_direct,
    deaths_indirect,
    injuries_direct,
    injuries_indirect,
    begin_date,
    end_date
)
SELECT
    col8::BIGINT,
    col9,
    col13,

    CASE
        WHEN col25 LIKE '%K' THEN REPLACE(col25,'K','')::FLOAT * 1000
        WHEN col25 LIKE '%M' THEN REPLACE(col25,'M','')::FLOAT * 1000000
        WHEN col25 LIKE '%B' THEN REPLACE(col25,'B','')::FLOAT * 1000000000
        WHEN col25 LIKE '%k' THEN REPLACE(col25,'k','')::FLOAT * 1000
        WHEN col25 LIKE '%m' THEN REPLACE(col25,'m','')::FLOAT * 1000000
        WHEN col25 LIKE '%b' THEN REPLACE(col25,'b','')::FLOAT * 1000000000
        WHEN col25 = '' OR col25 IS NULL THEN 0
        ELSE col25::FLOAT
    END,

    CASE
        WHEN col26 LIKE '%K' THEN REPLACE(col26,'K','')::FLOAT * 1000
        WHEN col26 LIKE '%M' THEN REPLACE(col26,'M','')::FLOAT * 1000000
        WHEN col26 LIKE '%B' THEN REPLACE(col26,'B','')::FLOAT * 1000000000
        WHEN col26 LIKE '%k' THEN REPLACE(col26,'k','')::FLOAT * 1000
        WHEN col26 LIKE '%m' THEN REPLACE(col26,'m','')::FLOAT * 1000000
        WHEN col26 LIKE '%b' THEN REPLACE(col26,'b','')::FLOAT * 1000000000
        WHEN col26 = '' OR col26 IS NULL THEN 0
        ELSE col26::FLOAT
    END,

    COALESCE(NULLIF(col23,''),'0')::INT,
    COALESCE(NULLIF(col24,''),'0')::INT,
    COALESCE(NULLIF(col21,''),'0')::INT,
    COALESCE(NULLIF(col22,''),'0')::INT,

    NULLIF(col18,'')::TIMESTAMP,
    NULLIF(col20,'')::TIMESTAMP

FROM storm_events_raw

WHERE
    (col25 IS NULL OR col25 = '' OR col25 ~ '^[0-9.]+[KkMmBb]?$')
    AND (col26 IS NULL OR col26 = '' OR col26 ~ '^[0-9.]+[KkMmBb]?$')
    AND col9 IS NOT NULL
    AND col9 <> '';
""")

cur.execute("""
UPDATE storm_events
SET S =
    damage_property
  + damage_crops
  + 1000000 * (deaths_direct + deaths_indirect)
  + 50000 * (injuries_direct + injuries_indirect);
""")

cur.execute("""
WITH stats AS (
    SELECT
        percentile_cont(0.25) WITHIN GROUP (ORDER BY S) AS p25,
        percentile_cont(0.75) WITHIN GROUP (ORDER BY S) AS p75,
        percentile_cont(0.95) WITHIN GROUP (ORDER BY S) AS p95
    FROM storm_events
    WHERE S > 0
)

UPDATE storm_events se
SET severity =
CASE
    WHEN se.S = 0 THEN 0
    WHEN se.S <= stats.p25 THEN 1
    WHEN se.S <= stats.p75 THEN 2
    WHEN se.S <= stats.p95 THEN 3
    ELSE 4
END
FROM stats;
""")

cur.execute("""
DROP TABLE IF EXISTS storm_events_raw CASCADE;
""")

cur = conn.cursor()

with open("sql/test_database.sql", encoding="utf-8") as f:
    for q in f.read().split(";"):
        if q.strip():
            cur.execute(q)
            pprint(cur.fetchall())
