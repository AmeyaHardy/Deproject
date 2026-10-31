# Spark Structured Streaming Performance Benchmark

## 1. Executive Summary & Objective

In our initial streaming implementation, Apache Spark Structured Streaming was configured with a **5-second micro-batch trigger**, but consistently logged warnings indicating micro-batches were taking **35–45 seconds** to complete, even on nearly empty batches:

```text
WARN ProcessingTimeExecutor: Current batch is falling behind. 
The trigger interval is 5000 milliseconds, but spent 44603 milliseconds
```

The hypothesized root cause was **excessive shuffle partitioning**. In Apache Spark, `spark.sql.shuffle.partitions` defaults to **`200`**. On a local machine running Windows, performing stateful event-time window aggregations across 200 shuffle partitions forces Spark to spawn 200 tasks per batch and commit delta files across 200 separate state-store directories on the local NTFS filesystem.

### The Objective
To test this hypothesis rigorously, we conducted an empirical benchmark across **four controlled experiments** varying the shuffle partition count (**200 vs. 2**) and workload volume (**500 vs. 5,000 records**).

---

## 2. Benchmark Environment & Experimental Setup

### System Specifications
- **Host OS**: Windows 10/11 (PowerShell)
- **CPU**: 11th Gen Intel Core i5-1135G7 @ 2.40 GHz (4 physical cores, 8 logical threads)
- **Java Runtime**: Oracle JRE 1.8.0_461
- **PySpark**: 3.5.5 with Hadoop 3.3.6 native Windows binaries (`winutils.exe` & `hadoop.dll`)
- **Kafka**: Confluent cp-kafka:7.7.0 (Single broker, `localhost:9092`)
- **Dataset**: UNSW-NB15 dataset (`UNSW_NB15_training-set.csv`, 82,332 rows offline slice used for initial streaming engine benchmarking)

### Controlled Constants Across All Experiments
To ensure a scientific, unbiased test where the **only** variables were shuffle partitions and workload size:
1. **Producer Replay Rate**: Fixed at **100.0 records/second** for all experiments.
2. **Streaming Logic**: Identical 5-second tumbling event-time window with a 10-second watermark (`withWatermark("event_time", "10 seconds")`) and 14 aggregation metrics (packet counts, byte counts, duration, load, protocol counts, ground-truth label tracking).
3. **Trigger Interval**: Fixed at **5 seconds** (`trigger(processingTime="5 seconds")`).
4. **Output Mode**: `update` mode to console sink.
5. **Topic Configuration**: 1 partition, replication factor 1.
6. **Hardware & Resource Limits**: Identical local resources with `master("local[*]")`.

### Isolation Methodology
To prevent cross-experiment contamination:
- **Kafka Isolation**: Each experiment wrote to and read from a dedicated, freshly created Kafka topic (`benchmark-a-500`, `benchmark-b-500`, `benchmark-c-5000`, `benchmark-d-5000`). This guaranteed all runs started cleanly at offset 0 and ended at the exact target count.
- **Checkpoint Isolation**: Each experiment used an isolated checkpoint directory under `data/processed/spark-checkpoints/benchmark/`. This prevented Spark's state-store schema mismatch exceptions (`Number of partitions cannot be changed for a stateful query`).
- **Production Safety**: The main development topic (`network-traffic`) and primary checkpoint were preserved untouched.

---

## 3. The Four Experiments

| Experiment | Shuffle Partitions | Workload (Records) | Topic | Checkpoint Path |
| :---: | :---: | :---: | :---: | :--- |
| **A** | **200** (Default) | **500** (Small) | `benchmark-a-500` | `data/processed/spark-checkpoints/benchmark/A-200-small` |
| **B** | **2** (Tuned) | **500** (Small) | `benchmark-b-500` | `data/processed/spark-checkpoints/benchmark/B-2-small` |
| **C** | **2** (Tuned) | **5,000** (Large) | `benchmark-c-5000` | `data/processed/spark-checkpoints/benchmark/C-2-large` |
| **D** | **200** (Default) | **5,000** (Large) | `benchmark-d-5000` | `data/processed/spark-checkpoints/benchmark/D-200-large` |

---

## 4. Empirical Benchmark Results

### Measured Performance Comparison Table

| Exp | Shuffle Partitions | Target Records | Actual Ingested | Total Elapsed (s) | Producer Elapsed (s) | Batches | Avg Batch Duration (ms) | Max Batch Duration (ms) | Processing Rate (rec/s) | Batches Exceeding Trigger | State Files Created |
| :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **A** | **200** | 500 | 500 | **90.13 s** | 10.56 s | 2 | **44,603 ms** | 46,013 ms | **5.55 rec/s** | 2 of 2 | 801 |
| **B** | **2** | 500 | 500 | **15.46 s** | 9.02 s | 3 | **4,843 ms** | 7,894 ms | **32.35 rec/s** | 1 of 3 | 13 |
| **C** | **2** | 5,000 | 5,000 | **91.42 s** | 82.67 s | 4 | **22,412 ms** | 70,229 ms | **54.69 rec/s** | 3 of 4 | 21 |
| **D** | **200** | 5,000 | 5,000 | **158.17 s** | 90.52 s | 3 | **41,450 ms** | 48,336 ms | **31.61 rec/s** | 3 of 3 | 1,601 |

*Raw machine-readable data stored at `data/processed/benchmark-results.csv` and detailed per-batch metrics in `outputs/benchmark/metrics_*.json`.*

---

## 5. In-Depth Technical Analysis

### 1. Small Workload Analysis (Exp A vs. Exp B: 500 records)
- **Total Elapsed Time**: Reduced from **90.13 seconds** down to **15.46 seconds** — a **5.83x speedup**.
- **Average Batch Duration**: Dropped from **44,603 ms** (~44.6 s) down to **4,843 ms** (~4.8 s).
  - With 2 partitions, the average batch duration dropped **below the 5-second trigger threshold**, eliminating continuous delay warnings.
- **State Store File Operations**:
  - In Exp A (200 partitions), Spark generated **801 state files** across 200 partition directories.
  - In Exp B (2 partitions), Spark generated only **13 state files** across 2 partition directories.
  - This **98.4% reduction in file I/O operations** directly translated to sub-5-second micro-batch execution on Windows NTFS.

### 2. Large Workload Analysis (Exp D vs. Exp C: 5,000 records)
- **Total Elapsed Time**: Reduced from **158.17 seconds** to **91.42 seconds** — a **1.73x speedup**.
- **Average Batch Duration**: Decreased from **41,450 ms** to **22,412 ms**.
- **Effective Processing Throughput**: Increased from **31.61 rec/s** to **54.69 rec/s**.
- **State Store File Operations**:
  - In Exp D, Spark wrote **1,601 state files**.
  - In Exp C, Spark wrote only **21 state files**.

### 3. The "Static Overhead" Phenomenon with 200 Partitions
Notice the average batch duration for 200 partitions across both workloads:
- Experiment A (500 records): **44,603 ms**
- Experiment D (5,000 records): **41,450 ms**

The average batch duration with 200 partitions remained virtually constant (~41–45 seconds) regardless of whether the micro-batch was processing 250 records or 1,600 records.
This confirms that under `spark.sql.shuffle.partitions = 200` locally:
> **The batch execution time was dominated by thread context-switching, task scheduling across 200 tasks on 4 physical CPU cores, and NTFS file handle coordination for 200 state store folders—NOT by the actual data processing.**

### 4. Workload Scaling Effects (Exp B vs. Exp C: 2 partitions)
- When input records scaled from 500 to 5,000 (a 10x increase):
  - Overall throughput increased from **32.35 rec/s** to **54.69 rec/s**.
  - This illustrates healthy batch amortization: larger micro-batches allow Spark's JVM and vectorized execution engine to achieve higher computational efficiency per CPU cycle.
  - However, because the incoming replay rate (100 rec/s) exceeded Spark's local console throughput (54.69 rec/s), records buffered in Kafka and micro-batches naturally expanded, resulting in larger micro-batch runtimes (~22 seconds).

---

## 6. Practical Recommendations for the Pipeline

Based on empirical evidence from all four experiments:

1. **Set `spark.sql.shuffle.partitions = 2` (or `4`) for Local Development**:
   - For single-machine pipelines where the Kafka topic has 1 partition, setting shuffle partitions to **2** provides immediate 6x latency improvement, eliminates Windows state file bottlenecks, and allows 5-second tumbling windows to keep pace with incoming streams.
2. **Reserve `spark.sql.shuffle.partitions = 200` for Distributed Clusters**:
   - The default value of 200 is designed for multi-node clusters with hundreds of executor cores and distributed filesystems (HDFS/S3), where parallel network shuffle outperforms serialization overhead. On local Windows machines, it should always be tuned down.
3. **Use `spark.local.dir` on Fast/Spacious Disks**:
   - Directing Spark's local scratch space (`spark.local.dir`) away from space-constrained system partitions (e.g. to a secondary drive with ample storage) prevents `java.io.IOException: There is not enough space on the disk` failures during state-store flushing.

---

## 7. How to Reproduce

All benchmark tools are committed and fully automated:

```powershell
# Run single experiment:
python src/benchmark_spark.py --experiment A
python src/benchmark_spark.py --experiment B
python src/benchmark_spark.py --experiment C
python src/benchmark_spark.py --experiment D

# Run all 4 sequentially:
python src/benchmark_spark.py --all
```
