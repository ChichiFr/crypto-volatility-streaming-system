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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
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
HISTORY_COLUMNS = [
    "timestamp",
    "symbol",
    "interval_start",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "source",
    "granularity",
]
FEATURE_COLUMNS = [
    "log_return",
    "rolling_volatility_60",
    "rolling_volatility_5",
    "rolling_volatility_22",
    "lag_1",
    "lag_5",
    "lag_22",
    "roc",
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

output_schema = StructType([
    StructField("prediction_timestamp", StringType(), True),
    StructField("market_timestamp", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("interval_start", LongType(), True),
    StructField("close", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("predicted_volatility_60min", DoubleType(), True),
    StructField("current_volatility", DoubleType(), True),
    StructField("rolling_volatility_5", DoubleType(), True),
    StructField("rolling_volatility_22", DoubleType(), True),
    StructField("log_return", DoubleType(), True),
    StructField("roc", DoubleType(), True),
    StructField("latency_seconds", DoubleType(), True),
    StructField("risk_signal", StringType(), True),
])


print("Connecting to Kafka...")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "crypto-stream")
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
print("Input: Kafka topic crypto-stream")
print("Model: XGBoost")
print(f"Output: {OUTPUT_PATH}")
print(f"Metrics: {METRICS_PATH}")
print("Trigger: Every 10 seconds")
print("=" * 80 + "\n")


history_by_symbol = {}


def compute_features(frame):
    frame = frame.sort_values(["interval_start", "timestamp"]).copy()
    numeric_columns = ["interval_start", "open", "high", "low", "close", "volume"]
    for column in numeric_columns:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")

    frame["prev_close"] = frame["close"].shift(1)
    frame["log_return"] = np.where(
        frame["prev_close"].notna(),
        np.log(frame["close"] / frame["prev_close"]),
        np.nan,
    )
    frame["rolling_volatility_60"] = frame["log_return"].rolling(60, min_periods=60).std()
    frame["rolling_volatility_5"] = frame["log_return"].rolling(5, min_periods=5).std()
    frame["rolling_volatility_22"] = frame["log_return"].rolling(22, min_periods=22).std()
    frame["lag_1"] = frame["close"].shift(1)
    frame["lag_5"] = frame["close"].shift(5)
    frame["lag_22"] = frame["close"].shift(22)
    frame["roc"] = np.where(
        frame["prev_close"].notna(),
        (frame["close"] - frame["prev_close"]) / frame["prev_close"],
        np.nan,
    )
    return frame


def apply_model(frame, symbol):
    feature_frame = frame[FEATURE_COLUMNS]

    if feature_frame.isnull().any(axis=None):
        frame["predicted_volatility_60min"] = np.nan
        return frame

    if symbol == "BTC-USD":
        model = model_btc
    elif symbol == "ETH-USD":
        model = model_eth
    else:
        frame["predicted_volatility_60min"] = np.nan
        return frame

    try:
        frame["predicted_volatility_60min"] = model.predict(feature_frame.to_numpy())
    except Exception as error:
        print(f"Prediction error for {symbol}: {error}")
        frame["predicted_volatility_60min"] = np.nan

    return frame


def label_risk(prediction_value):
    if pd.isna(prediction_value):
        return None
    if prediction_value > 0.02:
        return "HIGH"
    if prediction_value > 0.01:
        return "MEDIUM"
    return "LOW"


def process_batch(batch_df, batch_id):
    batch_start_time = time.time()

    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id}: no rows")
        return

    batch_pdf = batch_df.toPandas()
    if batch_pdf.empty:
        print(f"Batch {batch_id}: empty pandas batch")
        return

    batch_pdf["interval_start"] = batch_pdf["interval_start"].astype("int64")
    batch_pdf = batch_pdf.sort_values(["symbol", "interval_start", "timestamp"])

    output_frames = []

    for symbol, symbol_batch in batch_pdf.groupby("symbol", sort=False):
        history_pdf = history_by_symbol.get(symbol)
        if history_pdf is None:
            history_pdf = pd.DataFrame(columns=HISTORY_COLUMNS)

        combined_pdf = pd.concat(
            [history_pdf, symbol_batch[HISTORY_COLUMNS]],
            ignore_index=True,
        )
        combined_pdf = combined_pdf.sort_values(["interval_start", "timestamp"])
        combined_pdf = combined_pdf.drop_duplicates(
            subset=["symbol", "interval_start"],
            keep="last",
        )
        combined_pdf = compute_features(combined_pdf)

        current_intervals = symbol_batch["interval_start"].unique()
        current_rows = combined_pdf[combined_pdf["interval_start"].isin(current_intervals)].copy()
        current_rows = apply_model(current_rows, symbol)
        current_rows["risk_signal"] = current_rows["predicted_volatility_60min"].apply(label_risk)
        prediction_timestamp = pd.Timestamp.now(tz="UTC")
        current_rows["prediction_timestamp"] = prediction_timestamp.isoformat()
        current_rows["market_timestamp"] = current_rows["timestamp"]
        current_rows["current_volatility"] = current_rows["rolling_volatility_60"]
        market_timestamps = pd.to_datetime(current_rows["market_timestamp"], utc=True, errors="coerce")
        current_rows["latency_seconds"] = (prediction_timestamp - market_timestamps).dt.total_seconds()

        output_frames.append(
            current_rows[
                [
                    "prediction_timestamp",
                    "market_timestamp",
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
                ]
            ]
        )

        history_by_symbol[symbol] = combined_pdf[HISTORY_COLUMNS].tail(180).copy()

    if not output_frames:
        print(f"Batch {batch_id}: no output rows")
        return

    output_pdf = pd.concat(output_frames, ignore_index=True)
    output_pdf = output_pdf.where(pd.notnull(output_pdf), None)

    row_count = len(output_pdf.index)
    prediction_count = int(output_pdf["predicted_volatility_60min"].notna().sum())
    processing_time_ms = (time.time() - batch_start_time) * 1000.0
    latency_values = pd.to_numeric(output_pdf["latency_seconds"], errors="coerce").dropna().tolist()
    metrics.add_batch(row_count, prediction_count, processing_time_ms, latency_values)
    summary = metrics.summary()

    print(
        f"Batch {batch_id}: rows={row_count}, predictions={prediction_count}, "
        f"processing_ms={processing_time_ms:.2f}, avg_latency_s={summary['avg_latency_seconds']:.2f}"
    )
    if row_count:
        print(output_pdf.tail(min(5, row_count)).to_string(index=False))

    batch_df.sparkSession.createDataFrame(output_pdf, schema=output_schema).write.mode("append").parquet(
        OUTPUT_PATH
    )

    metrics_pdf = pd.DataFrame(
        [
            {
                "batch_id": int(batch_id),
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
    .trigger(processingTime="10 seconds")
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
