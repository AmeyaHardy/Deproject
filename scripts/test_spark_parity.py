"""
B3 Step 1 & 2: Spark Scoring & Sklearn Offline Parity Test (1,000 Records)
==========================================================================
Verifies that:
1. PySpark starts with the local JVM, HADOOP_HOME, JAVA_HOME, and workers load the model.
2. The PyArrow @pandas_udf produces predictions matching offline scikit-learn within 1e-6.
"""
import sys
import os
import json
import time
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Setup environment for Windows Spark
user_profile = os.environ.get("USERPROFILE", r"C:\Users\sweni")
hadoop_home = Path(user_profile) / "hadoop"
if hadoop_home.exists():
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    hadoop_bin = str(hadoop_home / "bin")
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

default_java = Path(r"C:\Program Files\Java\jre1.8.0_461")
if default_java.exists() and "JAVA_HOME" not in os.environ:
    os.environ["JAVA_HOME"] = str(default_java)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import joblib
import numpy as np
import pandas as pd
from src.raw_loader import iter_raw_records, FEATURE_NAMES
from src.anomaly.preprocessing import FrozenPreprocessor

SEED = 42
np.random.seed(SEED)
print(f"SEED: {SEED}")

# 1. Load 1,000 raw records
print("Loading 1,000 raw records...")
records = list(iter_raw_records(limit=1000, file_indices=[1]))
assert len(records) == 1000, f"Expected 1,000 records, got {len(records)}"

# 2. Score offline with scikit-learn
print("Computing offline predictions with scikit-learn...")
models_dir = ROOT / "data" / "processed" / "models"
preproc_dir = ROOT / "data" / "processed" / "preprocessing_artifacts"

rf_path = models_dir / "rf_detector.joblib"
pp_path = preproc_dir / "preprocessor.joblib"

rf_model = joblib.load(rf_path)
preprocessor = FrozenPreprocessor.load(preproc_dir)

feature_rows = [r["features"] for r in records]
record_ids = [r["record_id"] for r in records]

df_features = pd.DataFrame(feature_rows)[FEATURE_NAMES]
X_proc = preprocessor.transform(df_features)
offline_scores = rf_model.predict_proba(X_proc)[:, 1]

print(f"Offline scoring complete: min={offline_scores.min():.4f}, mean={offline_scores.mean():.4f}, max={offline_scores.max():.4f}")

# 3. Initialize PySpark
print("\nInitializing PySpark session...")
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

builder = (
    SparkSession.builder
    .appName("ParityTest")
    .master("local[2]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.showConsoleProgress", "false")
)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print("SparkSession created successfully.")

# 4. Define Pandas UDF
rf_path_str = str(rf_path.resolve())
pp_dir_str = str(preproc_dir.resolve())

_WORKER_MODEL = None
_WORKER_PREPROC = None

def get_worker_artifacts():
    global _WORKER_MODEL, _WORKER_PREPROC
    if _WORKER_MODEL is None:
        import joblib
        from src.anomaly.preprocessing import FrozenPreprocessor
        _WORKER_MODEL = joblib.load(rf_path_str)
        _WORKER_PREPROC = FrozenPreprocessor.load(pp_dir_str)
    return _WORKER_MODEL, _WORKER_PREPROC

@F.pandas_udf(DoubleType())
def predict_anomaly_udf(*cols: pd.Series) -> pd.Series:
    model, preproc = get_worker_artifacts()
    # Build dataframe with exact feature names
    pdf = pd.DataFrame(dict(zip(FEATURE_NAMES, cols)))
    pdf_proc = preproc.transform(pdf)
    scores = model.predict_proba(pdf_proc)[:, 1]
    return pd.Series(scores, dtype="float64")

# 5. Create Spark DataFrame
print("Creating Spark DataFrame from 1,000 records...")
spark_df = spark.createDataFrame(df_features)

# Apply UDF
scored_df = spark_df.withColumn(
    "spark_score",
    predict_anomaly_udf(*[F.col(c) for c in FEATURE_NAMES])
)

# Collect scores
print("Executing UDF in Spark workers...")
t0 = time.time()
spark_results = scored_df.select("spark_score").collect()
spark_time = time.time() - t0
spark_scores = np.array([r["spark_score"] for r in spark_results])

print(f"Spark scoring complete in {spark_time:.3f}s: min={spark_scores.min():.4f}, mean={spark_scores.mean():.4f}, max={spark_scores.max():.4f}")

# 6. Compare offline vs Spark scores
diff = np.abs(spark_scores - offline_scores)
max_diff = float(np.max(diff))
mean_diff = float(np.mean(diff))

print("\n" + "="*70)
print("PARITY TEST VERIFICATION")
print("="*70)
print(f"Max absolute difference:  {max_diff:.10e}")
print(f"Mean absolute difference: {mean_diff:.10e}")

assert max_diff < 1e-6, f"Parity test FAILED! max_diff = {max_diff}"
print("PARITY TEST PASSED: Spark scores match scikit-learn offline scores within 1e-6 across all 1,000 records!")

spark.stop()

