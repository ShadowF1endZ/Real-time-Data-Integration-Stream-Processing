# spark/streaming_job.py
"""
Spark Structured Streaming:
  - Kafka source (exactly-once via checkpointing + Kafka idempotent offset commit)
  - Tumbling window 60s  → aggregate stats per account
  - Sliding  window 30s/10s → velocity features
  - Watermark 5s để handle late data
  - ML scoring via broadcast model
  - Sink: alerts Kafka topic + PostgreSQL
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import pickle, json
from pyspark.sql.functions import pandas_udf
import pandas as pd
import numpy as np

import os 
from dotenv import load_dotenv
load_dotenv


spark = (
    SparkSession.builder
    .appName("FraudDetectionStreaming")
    .config("spark.sql.streaming.checkpointLocation", "/tmp/fraud_checkpoint")
    .config("spark.sql.shuffle.partitions", "8")
    # Kafka EOS
    .config("spark.kafka.consumer.group.id", "fraud-spark-group")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")


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


raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "latest")
    # Exactly-once: Spark tự quản lý offset qua checkpoint
    .option("kafka.isolation.level", "read_committed")
    .load()
)


events = (
    raw
    .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
    .select("d.*")
    .withColumn("event_time", (F.col("timestamp_ms") / 1000).cast(TimestampType()))
)


events_wm = events.withWatermark("event_time", "5 seconds")


tumbling_agg = (
    events_wm
    .groupBy(
        F.window("event_time", "60 seconds"),  
        "nameOrig"
    )
    .agg(
        F.count("*").alias("txn_count_60s"),
        F.sum("amount").alias("total_amount_60s"),
        F.max("amount").alias("max_amount_60s"),
        F.avg("amount").alias("avg_amount_60s"),
    )
)


sliding_agg = (
    events_wm
    .groupBy(
        F.window("event_time", "30 seconds", "10 seconds"),  # sliding
        "nameOrig"
    )
    .agg(
        F.count("*").alias("txn_velocity"),
        F.sum("amount").alias("amount_velocity"),
    )
)


TYPE_MAP = {"PAYMENT": 0, "TRANSFER": 1, "CASH_OUT": 2, "DEBIT": 3, "CASH_IN": 4}
type_map_expr = F.create_map([F.lit(k) for pair in TYPE_MAP.items() for k in pair])

features = (
    events
    .withColumn("type_enc",          type_map_expr[F.col("type")])
    .withColumn("balance_diff_orig",  F.col("oldbalanceOrg")  - F.col("newbalanceOrig"))
    .withColumn("balance_diff_dest",  F.col("newbalanceDest") - F.col("oldbalanceDest"))
    .withColumn("amount_ratio",       F.col("amount") / (F.col("oldbalanceOrg") + 1e-6))
    # Flag: balance về 0 sau giao dịch — dấu hiệu fraud phổ biến trong PaySim
    .withColumn("orig_balance_zero",  (F.col("newbalanceOrig") == 0).cast(IntegerType()))
    .withColumn("dest_balance_zero",  (F.col("newbalanceDest") == 0).cast(IntegerType()))
)


with open("/app/model/fraud_model.pkl", "rb") as f:
    _model = pickle.load(f)

model_broadcast = spark.sparkContext.broadcast(_model)

FEATURE_COLS = [
    "type_enc", "amount", "oldbalanceOrg", "newbalanceOrig",
    "oldbalanceDest", "newbalanceDest",
    "balance_diff_orig", "balance_diff_dest", "amount_ratio",
]

@pandas_udf(DoubleType())
def score_fraud(*cols) -> pd.Series:
    """Pandas UDF: vectorized scoring — efficient hơn row-by-row UDF."""
    X = pd.concat(cols, axis=1)
    X.columns = FEATURE_COLS
    model = model_broadcast.value
    proba = model.predict_proba(X.fillna(0))[:, 1]
    return pd.Series(proba)

scored = (
    features
    .withColumn(
        "fraud_score",
        score_fraud(*[F.col(c) for c in FEATURE_COLS])
    )
    .withColumn("is_fraud_pred", (F.col("fraud_score") > 0.5).cast(IntegerType()))
)

alerts = (
    scored
    .filter(F.col("is_fraud_pred") == 1)
    .select(
        "event_id", "nameOrig", "nameDest",
        "type", "amount", "fraud_score", "event_time"
    )
)


alert_kafka_query = (
    alerts
    .select(
        F.col("event_id").alias("key"),
        F.to_json(F.struct("*")).alias("value")
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("topic", "alerts")
    .option("checkpointLocation", "/tmp/fraud_checkpoint/alerts_kafka")
    .outputMode("append")
    .start()
)


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

pg_query = (
    scored.writeStream
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/tmp/fraud_checkpoint/postgres")
    .outputMode("append")
    .start()
)

# ──────────────────────────────────────────────
# Sink 3: Window aggregations → console (dev)
# Production: thay bằng PostgreSQL hoặc Redis
# ──────────────────────────────────────────────
tumbling_query = (
    tumbling_agg.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", False)
    .start()
)

spark.streams.awaitAnyTermination()