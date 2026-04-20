# spark/streaming_job.py
"""
Entry point của Spark job.
Chỉ làm một việc: kết nối ingest → feature → score → sink.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *


from feature_engineering import add_features
from ml_scorer import load_scorer

import os 
from dotenv import load_dotenv
load_dotenv()
# ──────────────────────────────────────────────
# Khởi tạo SparkSession
# ──────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("FraudDetectionStreaming")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/fraud_checkpoint")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ──────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────
SCHEMA = StructType([
    StructField("event_id",       StringType()),
    StructField("step",           IntegerType()),
    StructField("type",           StringType()),
    StructField("amount",         DoubleType()),
    StructField("nameOrig",       StringType()),
    StructField("oldbalanceOrg",  DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("nameDest",       StringType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()),
    StructField("timestamp_ms",   LongType()),
])

# ──────────────────────────────────────────────
# 1. Ingest — đọc từ Kafka
# ──────────────────────────────────────────────
raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    .option("kafka.isolation.level", "read_committed")
    .load()
)

events = (
    raw
    .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
    .select("d.*")
    .withColumn("event_time", (F.col("timestamp_ms") / 1000).cast(TimestampType()))
    .withWatermark("event_time", "5 seconds")
)

# ──────────────────────────────────────────────
# 2. Windowing aggregations
# ──────────────────────────────────────────────
tumbling_agg = (
    events
    .groupBy(F.window("event_time", "60 seconds"), "nameOrig")
    .agg(
        F.count("*").alias("txn_count_60s"),
        F.sum("amount").alias("total_amount_60s"),
        F.max("amount").alias("max_amount_60s"),
    )
)

sliding_agg = (
    events
    .groupBy(F.window("event_time", "30 seconds", "10 seconds"), "nameOrig")
    .agg(
        F.count("*").alias("txn_velocity"),
        F.sum("amount").alias("amount_velocity"),
    )
)

# ──────────────────────────────────────────────
# 3. Feature engineering + ML scoring
# ──────────────────────────────────────────────
features = add_features(events)

score_fraud, FEATURE_COLS = load_scorer(spark, "/app/model/fraud_model.pkl")

scored = (
    features
    .withColumn("fraud_score", score_fraud(*[F.col(c) for c in FEATURE_COLS]))
    .withColumn("is_fraud_pred", (F.col("fraud_score") > 0.5).cast(IntegerType()))
)

alerts = (
    scored
    .filter(F.col("is_fraud_pred") == 1)
    .select("event_id", "nameOrig", "nameDest", "type", "amount", "fraud_score", "event_time")
)

# ──────────────────────────────────────────────
# 4. Sinks
# ──────────────────────────────────────────────
def write_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    (
        batch_df.select(
            "event_id", "nameOrig", "nameDest", "type",
            "amount", "fraud_score", "is_fraud_pred", "event_time"
        )
        .write
        .format("jdbc")
        .option("url", f"jdbc:postgresql://postgres:5432/{os.getenv('POSTGRES_DB')}")
        .option("dbtable", "fraud_events")
        .option("user", os.getenv("POSTGRES_USER"))
        .option("password", os.getenv("POSTGRES_PASSWORD"))
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

# Sink 1: alerts → Kafka
alert_kafka_query = (
    alerts
    .select(F.col("event_id").alias("key"), F.to_json(F.struct("*")).alias("value"))
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "alerts")
    .option("checkpointLocation", "/tmp/fraud_checkpoint/alerts_kafka")
    .outputMode("append")
    .start()
)

# Sink 2: scored stream → PostgreSQL
pg_query = (
    scored.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/fraud_checkpoint/postgres")
    .outputMode("append")
    .start()
)

# Sink 3: window aggregations → console
tumbling_query = (
    tumbling_agg.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)

spark.streams.awaitAnyTermination()