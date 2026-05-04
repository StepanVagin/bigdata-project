"""Spark ML pipeline for storm event severity classification."""
# pylint: disable=import-error
import math
import os
from collections import defaultdict
from pprint import pprint

import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from tqdm import tqdm

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler,
    StandardScaler, VarianceThresholdSelector,
)
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

matplotlib.use("Agg")

TEAM = 29
WAREHOUSE = "project/hive/warehouse"

spark = SparkSession.builder \
    .appName(f"team{TEAM} - spark ML") \
    .master("yarn") \
    .config("hive.metastore.uris", "thrift://hadoop-02.uni.innopolis.ru:9883") \
    .config("spark.sql.warehouse.dir", WAREHOUSE) \
    .config("spark.sql.avro.compression.codec", "snappy") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.shuffle.partitions", "50") \
    .config("spark.driver.memory", "4g") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

STEPS = [
    "reading hive table",
    "feature engineering",
    "preprocessing pipeline",
    "train/test split",
    "RF cross-validation",
    "RF feature importance",
    "RF predictions & evaluation",
    "RF confusion matrix",
    "LR cross-validation",
    "LR predictions & evaluation",
    "LR confusion matrix",
    "model comparison chart",
    "saving CV metrics",
    "saving comparison",
]

pbar = tqdm(total=len(STEPS), ncols=70, bar_format="  {l_bar}{bar}| {n}/{total} [{elapsed}]")


def tick(label):
    """Advance the progress bar and show the completed step."""
    pbar.set_description(f"{label:<28}")
    pbar.update(1)


def run(cmd):
    """Run a shell command and return its output."""
    return os.popen(cmd).read()


def plot_confusion_matrix(pdf: pd.DataFrame, title: str, path: str) -> None:
    """Save a confusion-matrix heatmap for the given predictions DataFrame."""
    labels = sorted(pdf["label"].astype(int).unique())
    conf_matrix = (
        pd.crosstab(
            pdf["label"].astype(int),
            pdf["prediction"].astype(int),
            rownames=["Actual"],
            colnames=["Predicted"],
        )
        .reindex(index=labels, columns=labels, fill_value=0)
    )
    conf_fig, conf_ax = plt.subplots(figsize=(6, 5))
    sns.heatmap(conf_matrix, annot=True, fmt="d", cmap="Blues", ax=conf_ax)
    conf_ax.set_title(title)
    plt.tight_layout()
    conf_fig.savefig(path, dpi=120)
    plt.close(conf_fig)


run("mkdir -p data output models")

df = spark.read.format("avro").table("team29_projectdb.storm_events_part_buck")
df.printSchema()
tick("reading hive table")

# Extract cyclical time features; exclude S-derived columns (data leakage)
df = (
    df.withColumn("year",      F.year("begin_date"))
      .withColumn("month",     F.month("begin_date"))
      .withColumn("day",       F.dayofmonth("begin_date"))
      .withColumn("hour",      F.hour("begin_date"))
      .withColumn("month_sin", F.sin(2 * math.pi * F.col("month") / 12))
      .withColumn("month_cos", F.cos(2 * math.pi * F.col("month") / 12))
      .withColumn("day_sin",   F.sin(2 * math.pi * F.col("day") / 31))
      .withColumn("day_cos",   F.cos(2 * math.pi * F.col("day") / 31))
      .withColumn("hour_sin",  F.sin(2 * math.pi * F.col("hour") / 24))
      .withColumn("hour_cos",  F.cos(2 * math.pi * F.col("hour") / 24))
      .withColumn("label",     F.col("severity").cast("double"))
)

FEATURE_COLS = [
    "event_type", "state",
    "year", "month_sin", "month_cos", "day_sin", "day_cos", "hour_sin", "hour_cos",
]
df = df.select(FEATURE_COLS + ["label"]).na.drop()
tick("feature engineering")

cat_cols = ["event_type", "state"]
num_cols = ["year", "month_sin", "month_cos", "day_sin", "day_cos", "hour_sin", "hour_cos"]

indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="skip")
    for c in cat_cols
]
encoders = [
    OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_enc")
    for c in cat_cols
]
# Assemble and scale numerical features separately (Note2)
num_assembler = VectorAssembler(inputCols=num_cols, outputCol="num_raw")
scaler = StandardScaler(
    inputCol="num_raw", outputCol="num_scaled", withMean=True, withStd=True
)
# Combine OHE categoricals with scaled numerics
assembler = VectorAssembler(
    inputCols=[f"{c}_enc" for c in cat_cols] + ["num_scaled"],
    outputCol="raw_features",
)
# Remove zero-variance features (e.g. unseen OHE columns) — Note4
selector = VarianceThresholdSelector(
    featuresCol="raw_features", outputCol="features", varianceThreshold=0.0
)

prep_pipeline = Pipeline(stages=indexers + encoders + [num_assembler, scaler, assembler, selector])
prep_model = prep_pipeline.fit(df)
data = prep_model.transform(df).select("features", "label").cache()
data.show(5)
tick("preprocessing pipeline")

train_data, test_data = data.randomSplit([0.7, 0.3], seed=42)

# Repartition to match cluster parallelism so all cores stay busy during CV
num_parts = spark.sparkContext.defaultParallelism * 2
train_data = train_data.repartition(num_parts)
test_data = test_data.repartition(num_parts // 2)

train_data.cache()
test_data.cache()

# Materialize both splits into cache so all CV folds read from memory
train_count = train_data.count()
test_count = test_data.count()
print(f"Train size: {train_count}  Test size: {test_count}  Partitions: {num_parts}")

num_classes = 5
class_counts_df = (
    train_data.groupBy("label")
    .agg(F.count("*").alias("cnt"))
    .withColumn("class_weight", F.lit(float(train_count)) / (F.lit(num_classes) * F.col("cnt")))
    .select("label", "class_weight")
)
train_data = train_data.join(F.broadcast(class_counts_df), on="label").cache()

# Save to HDFS (required by checklist) — no coalesce, write in parallel from cache
train_data.write.mode("overwrite").format("json").save("project/data/train")
run("hdfs dfs -cat project/data/train/*.json > data/train.json")
test_data.write.mode("overwrite").format("json").save("project/data/test")
run("hdfs dfs -cat project/data/test/*.json > data/test.json")

tick("train/test split")

evaluator_f1 = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="f1"
)
evaluator_acc = MulticlassClassificationEvaluator(
    labelCol="label", predictionCol="prediction", metricName="accuracy"
)

# Model 1: Random Forest
rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    weightCol="class_weight",
    seed=42,
    maxBins=16,
    subsamplingRate=0.8,
    cacheNodeIds=True,
)

# numTrees, maxDepth = model HPs; featureSubsetStrategy = algorithm HP (random subspace method)
grid_rf = (
    ParamGridBuilder()
    .addGrid(rf.numTrees,             [10, 20, 50])
    .addGrid(rf.maxDepth,             [3, 5, 7])
    .addGrid(rf.featureSubsetStrategy, ["auto", "sqrt", "log2"])
    .build()
)

cv_rf = CrossValidator(
    estimator=rf,
    estimatorParamMaps=grid_rf,
    evaluator=evaluator_f1,
    numFolds=3,
    parallelism=8,
    seed=42,
)

cv_model_rf = cv_rf.fit(train_data)
model1 = cv_model_rf.bestModel
pprint(model1.extractParamMap())
tick("RF cross-validation")

# Aggregate feature importances by original column (OHE expands cats into many indices)
meta_attrs = data.schema["features"].metadata.get("ml_attr", {}).get("attrs", {})
idx_to_orig = {}
for group in meta_attrs.values():
    for attr in group:
        name = attr["name"]
        idx_to_orig[attr["idx"]] = name.split("_enc")[0] if "_enc" in name else name

raw_imp = model1.featureImportances.toArray()
agg_imp = defaultdict(float)
for idx, imp in enumerate(raw_imp):
    agg_imp[idx_to_orig.get(idx, f"feature_{idx}")] += float(imp)

fi_df = spark.createDataFrame(
    sorted(agg_imp.items(), key=lambda x: -x[1]),
    ["feature", "importance"],
)
fi_df.show()
(
    fi_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("sep", ",")
    .option("header", "true")
    .save("project/output/feature_importance")
)
run("hdfs dfs -cat project/output/feature_importance/*.csv > output/feature_importance.csv")
tick("RF feature importance")

model1.write().overwrite().save("project/models/model1")
run("rm -rf models/model1 && hdfs dfs -get project/models/model1 models/model1")

predictions1 = model1.transform(test_data)
(
    predictions1.select("label", "prediction")
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("sep", ",")
    .option("header", "true")
    .save("project/output/model1_predictions")
)
run("hdfs dfs -cat project/output/model1_predictions/*.csv > output/model1_predictions.csv")

f1_1  = evaluator_f1.evaluate(predictions1)
acc_1 = evaluator_acc.evaluate(predictions1)
tick("RF predictions & evaluation")
print(f"Model 1 (RandomForest)  — F1: {f1_1:.4f}  Accuracy: {acc_1:.4f}")

pdf1 = predictions1.select("label", "prediction").toPandas()
plot_confusion_matrix(
    pdf1, "Random Forest — Confusion Matrix", "output/model1_confusion_matrix.png"
)
tick("RF confusion matrix")

# Model 2: Logistic Regression
lr = LogisticRegression(labelCol="label", featuresCol="features", weightCol="class_weight", maxIter=50)

# regParam = algorithm HP (regularisation strength)
# elasticNetParam, aggregationDepth = model HPs
grid_lr = (
    ParamGridBuilder()
    .addGrid(lr.regParam,         [0.01, 0.1, 1.0])
    .addGrid(lr.elasticNetParam,  [0.0, 0.5, 1.0])
    .addGrid(lr.aggregationDepth, [2, 3, 4])
    .build()
)

cv_lr = CrossValidator(
    estimator=lr,
    estimatorParamMaps=grid_lr,
    evaluator=evaluator_f1,
    numFolds=3,
    parallelism=8,
    seed=42,
)

cv_model_lr = cv_lr.fit(train_data)
model2 = cv_model_lr.bestModel
pprint(model2.extractParamMap())
tick("LR cross-validation")

model2.write().overwrite().save("project/models/model2")
run("rm -rf models/model2 && hdfs dfs -get project/models/model2 models/model2")

predictions2 = model2.transform(test_data)
(
    predictions2.select("label", "prediction")
    .coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("sep", ",")
    .option("header", "true")
    .save("project/output/model2_predictions")
)
run("hdfs dfs -cat project/output/model2_predictions/*.csv > output/model2_predictions.csv")

f1_2  = evaluator_f1.evaluate(predictions2)
acc_2 = evaluator_acc.evaluate(predictions2)
tick("LR predictions & evaluation")
print(f"Model 2 (LogisticRegression) — F1: {f1_2:.4f}  Accuracy: {acc_2:.4f}")

pdf2 = predictions2.select("label", "prediction").toPandas()
plot_confusion_matrix(
    pdf2, "Logistic Regression — Confusion Matrix", "output/model2_confusion_matrix.png"
)
tick("LR confusion matrix")

cv_df = spark.createDataFrame(
    [
        ("Random Forest",       float(max(cv_model_rf.avgMetrics))),
        ("Logistic Regression", float(max(cv_model_lr.avgMetrics))),
    ],
    ["model", "best_cv_f1"],
)
cv_df.show()
(
    cv_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("sep", ",")
    .option("header", "true")
    .save("project/output/cv_metrics")
)
run("hdfs dfs -cat project/output/cv_metrics/*.csv > output/cv_metrics.csv")
tick("saving CV metrics")

MODELS_LABELS = ["Random Forest", "Logistic Regression"]
X_POS = [0, 1]
WIDTH = 0.3
comp_fig, comp_ax = plt.subplots(figsize=(6, 4))
comp_ax.bar([i - WIDTH / 2 for i in X_POS], [f1_1, f1_2],  WIDTH, label="F1")
comp_ax.bar([i + WIDTH / 2 for i in X_POS], [acc_1, acc_2], WIDTH, label="Accuracy")
comp_ax.set_xticks(X_POS)
comp_ax.set_xticklabels(MODELS_LABELS)
comp_ax.set_ylim(0, 1)
comp_ax.set_ylabel("Score")
comp_ax.set_title("Model Comparison — F1 vs Accuracy")
comp_ax.legend()
plt.tight_layout()
comp_fig.savefig("output/model_comparison.png", dpi=120)
plt.close(comp_fig)
tick("model comparison chart")

comparison = [
    ["Random Forest",       f1_1, acc_1],
    ["Logistic Regression", f1_2, acc_2],
]
eval_df = spark.createDataFrame(comparison, ["model", "F1", "Accuracy"])
eval_df.show(truncate=False)

(
    eval_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("sep", ",")
    .option("header", "true")
    .save("project/output/evaluation")
)
run("hdfs dfs -cat project/output/evaluation/*.csv > output/evaluation.csv")
tick("saving comparison")

pbar.close()
spark.stop()
