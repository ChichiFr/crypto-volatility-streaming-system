import sys

from pyspark.sql import SparkSession

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:
        pass

spark = (
    SparkSession.builder
    .appName("CryptoVolatilityStreaming")
    .master("local[*]")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark_checkpoint")
    .getOrCreate()
)

print(f"PySpark {spark.version} is working!")
spark.stop()
