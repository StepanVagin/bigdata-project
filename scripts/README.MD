# Scripts

End-to-end pipeline for ingesting, transforming, and modelling NOAA Storm Events data on a Hadoop/Hive cluster.

## Pipeline overview

```
stage1.sh  →  stage2.sh  →  stage3.sh  →  stage4.sh
  (ingest)     (hive EDA)    (spark ML)    (postprocess)
```

Run each stage from the **project root** (not from inside `scripts/`).

---

## Stages

### Stage 1 — `stage1.sh`
Bootstraps the PostgreSQL database and imports raw data into HDFS via Sqoop.

1. Runs `build_projectdb.py` to create and populate the PostgreSQL database.
2. Runs `data_ingestion.sh` to export all tables from PostgreSQL into HDFS as Avro files (Snappy-compressed).
3. Lints `build_projectdb.py` with pylint (non-blocking).

**Requires:** `secrets/.psql.pass`, raw CSV files in `data/`, SQL files in `sql/`.

---

### Stage 2 — `stage2.sh`
Builds Hive tables and runs 6 analytical queries.

1. Uploads Avro schemas to `project/warehouse/avsc/` in HDFS.
2. Executes `sql/db.hql` via Beeline to create the Hive database/tables.
3. Runs `sql/q1.hql` – `sql/q6.hql` and writes results to `output/q{1..6}.csv`.

| Output file | Query |
|---|---|
| `output/q1.csv` | Financial damage by event type |
| `output/q2.csv` | Event count, avg damage, and deaths by severity level |
| `output/q3.csv` | Extreme event count by state |
| `output/q4.csv` | Avg severity and total damage by year |
| `output/q5.csv` | Total deaths and damage by event type |
| `output/q6.csv` | Event count by month |

**Requires:** `secrets/.hive.pass`, HDFS access, Beeline.

---

### Stage 3 — `stage3.sh`
Runs the Spark ML pipeline on YARN.

1. Sets `HADOOP_CONF_DIR` and `YARN_CONF_DIR`.
2. Submits `model.py` via `spark-submit --master yarn`.
3. Lints `model.py` with pylint (non-blocking).

See [`model.py`](#modelpy) for full ML details.

**Requires:** `secrets/.hive.pass`, YARN cluster, Hive metastore.

---

### Stage 4 — `stage4.sh`
Post-processing stage (currently a placeholder — no operations defined).

---

## Supporting scripts

### `preprocess.sh`
Downloads raw NOAA Storm Events CSV files and a UGC areas reference file.

- Recursively fetches all `*.csv.gz` files from the NOAA SWDI archive.
- Decompresses them into `data/`.
- Downloads `ugc_areas.csv` into `data/`.

### `data_collection.sh`
Thin wrapper — delegates to `preprocess.sh`.

### `data_ingestion.sh`
Exports the PostgreSQL database to HDFS using Sqoop.

- Removes and recreates `/user/team29/project/warehouse` in HDFS.
- Runs `sqoop import-all-tables` with Snappy+Avro output into `project/warehouse`.
- Copies generated `.avsc` / `.java` files to `output/` and removes them from the working directory.

### `postprocess.sh`
Registers ML results in Hive and loads them into PostgreSQL for Superset dashboards.

1. Creates five external Hive tables pointing at HDFS CSV output directories:
   `model1_predictions`, `model2_predictions`, `ml_evaluation`, `feature_importance`, `cv_metrics`.
2. Recreates matching tables in PostgreSQL and bulk-copies data from local CSV files.
3. Prints row counts from PostgreSQL to verify the load.

**Requires:** `secrets/.hive.pass`, `secrets/.psql.pass`, output CSVs in `output/`.

---

## Python scripts

### `build_projectdb.py`
ETL script that builds the PostgreSQL `team29_projectdb` database.

1. Executes `sql/create_tables.sql` to set up schema.
2. Bulk-loads all `data/StormEvents_details*.csv` files into a raw staging table via `COPY`.
3. Transforms and inserts cleaned records into `storm_events`, parsing damage strings like `1.5K`, `2M`, `3B` into numeric values.
4. Computes a composite severity score **S** = `damage_property + damage_crops + 1,000,000 × deaths + 50,000 × injuries`.
5. Assigns a 0–4 **severity** label using quartile/percentile thresholds (p25, p75, p95) on S.
6. Drops the raw staging table.

### `model.py`
Spark ML pipeline that classifies storm event severity (5 classes: 0–4).

**Features used:** `event_type`, `state`, `year`, and cyclical encodings of month/day/hour.

**Pipeline:**
1. StringIndexer + OneHotEncoder for categorical columns.
2. StandardScaler for numeric columns.
3. VarianceThresholdSelector to drop zero-variance OHE columns.

**Models trained with 3-fold cross-validation:**

| Model | Hyperparameters searched |
|---|---|
| Random Forest | `numTrees` ∈ {10,20,50}, `maxDepth` ∈ {3,5,7}, `featureSubsetStrategy` ∈ {auto,sqrt,log2} |
| Logistic Regression | `regParam` ∈ {0.01,0.1,1.0}, `elasticNetParam` ∈ {0.0,0.5,1.0}, `aggregationDepth` ∈ {2,3,4} |

**Outputs written to `output/`:**

| File | Description |
|---|---|
| `model1_predictions.csv` | RF label vs prediction on test set |
| `model2_predictions.csv` | LR label vs prediction on test set |
| `evaluation.csv` | F1 and accuracy for both models |
| `feature_importance.csv` | Aggregated RF feature importances |
| `cv_metrics.csv` | Best cross-validation F1 per model |
| `model1_confusion_matrix.png` | RF confusion matrix heatmap |
| `model2_confusion_matrix.png` | LR confusion matrix heatmap |
| `model_comparison.png` | Side-by-side F1/accuracy bar chart |

Models are also saved to HDFS under `project/models/model1` and `project/models/model2`.

---

## Secrets

| File | Used by |
|---|---|
| `secrets/.psql.pass` | `build_projectdb.py`, `data_ingestion.sh`, `postprocess.sh` |
| `secrets/.hive.pass` | `stage2.sh`, `stage3.sh`, `postprocess.sh` |

