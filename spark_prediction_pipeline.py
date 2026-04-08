"""
Spark Structured Streaming - Real-time crypto predictions.
Reads candles from Kafka, computes features per micro-batch, applies XGBoost,
and writes predictions to Parquet.
"""

import os
import pickle
import sys
import time

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    avg,
    col,
    current_timestamp,
    desc,
    from_json,
    lag,
    log,
    pandas_udf,
    row_number,
    stddev,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass


OUTPUT_PATH = "data/predictions"
METRICS_PATH = "data/metrics"
CHECKPOINT_PATH = "/tmp/spark_checkpoint_predictions"
HISTORY_PATH = "/tmp/crypto_raw_history"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "crypto-stream")
FEATURE_COLUMNS = [
    "realized_vol_5",
    "realized_vol_22",
    "realized_vol_60",
    "lag_1_vol",
    "lag_5_vol",
    "volume_rolling_mean",
    "roc_5",
    "roc_22",
]

os.makedirs(OUTPUT_PATH, exist_ok=True)
os.makedirs(METRICS_PATH, exist_ok=True)


class PerformanceMetrics:
    def __init__(self):
        self.start_time = time.time()
        self.batch_count = 0
        self.total_messages = 0
        self.total_predictions = 0
        self.processing_times_ms = []
        self.latencies_seconds = []

    def add_batch(self, message_count, prediction_count, processing_time_ms, latency_values):
        self.batch_count += 1
        self.total_messages += int(message_count)
        self.total_predictions += int(prediction_count)
        self.processing_times_ms.append(float(processing_time_ms))
        self.latencies_seconds.extend(float(value) for value in latency_values if pd.notna(value))

    def summary(self):
        runtime_seconds = time.time() - self.start_time
        throughput = self.total_messages / runtime_seconds if runtime_seconds > 0 else 0.0

        processing_mean = float(np.mean(self.processing_times_ms)) if self.processing_times_ms else 0.0
        processing_p95 = (
            float(np.percentile(self.processing_times_ms, 95)) if self.processing_times_ms else 0.0
        )
        latency_mean = float(np.mean(self.latencies_seconds)) if self.latencies_seconds else 0.0
        latency_p95 = float(np.percentile(self.latencies_seconds, 95)) if self.latencies_seconds else 0.0

        return {
            "runtime_seconds": runtime_seconds,
            "batch_count": self.batch_count,
            "total_messages": self.total_messages,
            "total_predictions": self.total_predictions,
            "throughput_msg_per_sec": throughput,
            "avg_processing_time_ms": processing_mean,
            "p95_processing_time_ms": processing_p95,
            "avg_latency_seconds": latency_mean,
            "p95_latency_seconds": latency_p95,
        }


metrics = PerformanceMetrics()

print("Starting Spark Streaming with XGBoost predictions...\n")
print("Loading XGBoost models...")

with open("models/model_btc.pkl", "rb") as file_handle:
    model_btc = pickle.load(file_handle)
    print("BTC model loaded")

with open("models/model_eth.pkl", "rb") as file_handle:
    model_eth = pickle.load(file_handle)
    print("ETH model loaded\n")


@pandas_udf(DoubleType())
def predict_volatility_udf(
    symbol_series,
    realized_vol_5_series,
    realized_vol_22_series,
    realized_vol_60_series,
    lag_1_vol_series,
    lag_5_vol_series,
    volume_rolling_mean_series,
    roc_5_series,
    roc_22_series,
):
    predictions = []

    for symbol, realized_vol_5, realized_vol_22, realized_vol_60, lag_1_vol, lag_5_vol, volume_rolling_mean, roc_5, roc_22 in zip(
        symbol_series,
        realized_vol_5_series,
        realized_vol_22_series,
        realized_vol_60_series,
        lag_1_vol_series,
        lag_5_vol_series,
        volume_rolling_mean_series,
        roc_5_series,
        roc_22_series,
    ):
        if any(
            pd.isna(value)
            for value in [
                realized_vol_5,
                realized_vol_22,
                realized_vol_60,
                lag_1_vol,
                lag_5_vol,
                volume_rolling_mean,
                roc_5,
                roc_22,
            ]
        ):
            predictions.append(None)
            continue

        features = np.array(
            [
                realized_vol_5,
                realized_vol_22,
                realized_vol_60,
                lag_1_vol,
                lag_5_vol,
                volume_rolling_mean,
                roc_5,
                roc_22,
            ],
            dtype=np.float32,
        ).reshape(1, -1)

        if symbol == "BTC-USD":
            pred = model_btc.predict(features)[0]
        elif symbol == "ETH-USD":
            pred = model_eth.predict(features)[0]
        else:
            pred = None

        predictions.append(float(pred) if pred is not None else None)

    return pd.Series(predictions)


spark = (
    SparkSession.builder
    .appName("CryptoVolatilityPredictions")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print(f"Spark {spark.version} started\n")


input_schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("interval_start", LongType(), True),
    StructField("open", DoubleType(), True),
    StructField("high", DoubleType(), True),
    StructField("low", DoubleType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("source", StringType(), True),
    StructField("granularity", StringType(), True),
])


print("Connecting to Kafka...")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

stream_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), input_schema).alias("data"))
    .select("data.*")
)

print("Connected to Kafka")
print("Prediction pipeline configured")
print("=" * 80)
print("REAL-TIME PREDICTIONS ACTIVE")
print("=" * 80)
print(f"Input: Kafka topic {KAFKA_TOPIC}")
print("Model: XGBoost")
print(f"Kafka Broker: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"Output: {OUTPUT_PATH}")
print(f"Metrics: {METRICS_PATH}")
print("Trigger: Every 10 seconds")
print("=" * 80 + "\n")


def process_batch(batch_df, batch_id):
    batch_start_time = time.time()

    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: no rows")
        return

    batch_df = batch_df.withColumn("interval_start", col("interval_start").cast("long"))
    batch_df = batch_df.withColumn("open", col("open").cast("double"))
    batch_df = batch_df.withColumn("high", col("high").cast("double"))
    batch_df = batch_df.withColumn("low", col("low").cast("double"))
    batch_df = batch_df.withColumn("close", col("close").cast("double"))
    batch_df = batch_df.withColumn("volume", col("volume").cast("double"))
    current_batch_df = batch_df.select(*input_schema.fieldNames()).dropDuplicates(["symbol", "interval_start"])
    current_keys_df = current_batch_df.select("symbol", "interval_start")

    if os.path.exists(HISTORY_PATH):
        history_df = batch_df.sparkSession.read.schema(input_schema).parquet(HISTORY_PATH)
    else:
        history_df = batch_df.sparkSession.createDataFrame([], input_schema)

    combined_df = history_df.unionByName(current_batch_df)

    dedupe_window = Window.partitionBy("symbol", "interval_start").orderBy(desc("timestamp"))
    combined_df = (
        combined_df.withColumn("row_num", row_number().over(dedupe_window))
        .where(col("row_num") == 1)
        .drop("row_num")
    )

    feature_window = Window.partitionBy("symbol").orderBy("interval_start", "timestamp")
    rolling_window_60 = feature_window.rowsBetween(-59, 0)
    rolling_window_5 = feature_window.rowsBetween(-4, 0)
    rolling_window_22 = feature_window.rowsBetween(-21, 0)

    combined_df = combined_df.withColumn("prev_close", lag("close", 1).over(feature_window))

    combined_df = combined_df.withColumn(
        "log_return",
        when(col("prev_close").isNotNull(), log(col("close") / col("prev_close"))).otherwise(None),
    )
    combined_df = combined_df.withColumn("realized_vol_60", stddev("log_return").over(rolling_window_60))
    combined_df = combined_df.withColumn("realized_vol_5", stddev("log_return").over(rolling_window_5))
    combined_df = combined_df.withColumn("realized_vol_22", stddev("log_return").over(rolling_window_22))
    combined_df = combined_df.withColumn("lag_1_vol", lag("realized_vol_60", 1).over(feature_window))
    combined_df = combined_df.withColumn("lag_5_vol", lag("realized_vol_60", 5).over(feature_window))
    combined_df = combined_df.withColumn("volume_rolling_mean", avg("volume").over(rolling_window_5))
    combined_df = combined_df.withColumn("lag_close_5", lag("close", 5).over(feature_window))
    combined_df = combined_df.withColumn("lag_close_22", lag("close", 22).over(feature_window))
    combined_df = combined_df.withColumn(
        "roc_5",
        when(
            col("lag_close_5").isNotNull(),
            (col("close") - col("lag_close_5")) / col("lag_close_5"),
        ).otherwise(None),
    )
    combined_df = combined_df.withColumn(
        "roc_22",
        when(
            col("lag_close_22").isNotNull(),
            (col("close") - col("lag_close_22")) / col("lag_close_22"),
        ).otherwise(None),
    )

    batch_df = combined_df.join(current_keys_df, on=["symbol", "interval_start"], how="inner")

    batch_df = batch_df.withColumn(
        "predicted_volatility_60min",
        predict_volatility_udf(
            col("symbol"),
            col("realized_vol_5"),
            col("realized_vol_22"),
            col("realized_vol_60"),
            col("lag_1_vol"),
            col("lag_5_vol"),
            col("volume_rolling_mean"),
            col("roc_5"),
            col("roc_22"),
        ),
    )
    batch_df = batch_df.withColumn("prediction_timestamp", current_timestamp())
    batch_df = batch_df.withColumn("market_timestamp", to_timestamp(col("timestamp")))
    batch_df = batch_df.withColumn("current_volatility", col("realized_vol_60"))
    batch_df = batch_df.withColumn("rolling_volatility_5", col("realized_vol_5"))
    batch_df = batch_df.withColumn("rolling_volatility_22", col("realized_vol_22"))
    batch_df = batch_df.withColumn("roc", col("roc_5"))
    batch_df = batch_df.withColumn(
        "latency_seconds",
        when(
            col("market_timestamp").isNotNull(),
            col("prediction_timestamp").cast("double") - col("market_timestamp").cast("double"),
        ).otherwise(None),
    )
    batch_df = batch_df.withColumn(
        "risk_signal",
        when(col("predicted_volatility_60min") > 0.02, "HIGH")
        .when(col("predicted_volatility_60min") > 0.01, "MEDIUM")
        .when(col("predicted_volatility_60min").isNotNull(), "LOW")
        .otherwise(None),
    )

    output_df = batch_df.select(
        col("prediction_timestamp").cast("string").alias("prediction_timestamp"),
        col("market_timestamp").cast("string").alias("market_timestamp"),
        "symbol",
        "interval_start",
        "close",
        "volume",
        "predicted_volatility_60min",
        "current_volatility",
        "rolling_volatility_5",
        "rolling_volatility_22",
        "log_return",
        "roc",
        "latency_seconds",
        "risk_signal",
    )

    row_count = output_df.count()
    prediction_count = output_df.filter(col("predicted_volatility_60min").isNotNull()).count()
    processing_time_ms = (time.time() - batch_start_time) * 1000.0
    latency_values = [
        row["latency_seconds"]
        for row in output_df.select("latency_seconds").where(col("latency_seconds").isNotNull()).collect()
    ]

    metrics.add_batch(row_count, prediction_count, processing_time_ms, latency_values)
    summary = metrics.summary()

    print(
        f"Batch {batch_id}: rows={row_count}, predictions={prediction_count}, "
        f"processing_ms={processing_time_ms:.2f}, avg_latency_s={summary['avg_latency_seconds']:.2f}"
    )
    if row_count:
        output_df.show(min(5, row_count), truncate=False)

    output_df.write.mode("append").parquet(OUTPUT_PATH)

    history_tail_window = Window.partitionBy("symbol").orderBy(desc("interval_start"))
    history_df = (
        combined_df.select(*input_schema.fieldNames())
        .withColumn("history_rank", row_number().over(history_tail_window))
        .where(col("history_rank") <= 180)
        .drop("history_rank")
    )
    history_df.write.mode("overwrite").parquet(HISTORY_PATH)

    metrics_pdf = pd.DataFrame(
        [
            {
                "batch_id": int(batch_id),
                "timestamp": pd.Timestamp.now("UTC").isoformat(),
                "runtime_seconds": summary["runtime_seconds"],
                "total_messages": summary["total_messages"],
                "total_predictions": summary["total_predictions"],
                "throughput_msg_per_sec": summary["throughput_msg_per_sec"],
                "avg_processing_time_ms": summary["avg_processing_time_ms"],
                "p95_processing_time_ms": summary["p95_processing_time_ms"],
                "avg_latency_seconds": summary["avg_latency_seconds"],
                "p95_latency_seconds": summary["p95_latency_seconds"],
            }
        ]
    )
    batch_df.sparkSession.createDataFrame(metrics_pdf).write.mode("append").parquet(METRICS_PATH)


query = (
    stream_df.writeStream
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .foreachBatch(process_batch)
    .trigger(processingTime="1 minutes")
    .start()
)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nStopping stream...")
    final_summary = metrics.summary()
    print(
        "Final metrics: "
        f"batches={final_summary['batch_count']}, "
        f"messages={final_summary['total_messages']}, "
        f"predictions={final_summary['total_predictions']}, "
        f"throughput={final_summary['throughput_msg_per_sec']:.3f} msg/s, "
        f"avg_processing={final_summary['avg_processing_time_ms']:.2f} ms, "
        f"avg_latency={final_summary['avg_latency_seconds']:.2f} s"
    )
    query.stop()
    spark.stop()
    print("Stopped gracefully")
