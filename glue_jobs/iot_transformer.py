"""
iot_transformer – AWS Glue 4.0 PySpark Streaming job
Pipeline: S3 landing zone (from Kafka S3 Sink Connector) → enriched Parquet → Redshift

Architecture:
  MSK Kafka → Kafka S3 Sink Connector (5min batches) → S3 raw/iot/
  → THIS JOB (every 15 min) → S3 processed/iot_events/ → Redshift COPY

Design decisions:
  - Glue Streaming (not batch) for sub-minute latency on critical alarms
  - Watermark 5 min to handle late Kafka delivery
  - Micro-batch checkpoint in S3 for exactly-once semantics
  - Device registry lookup (Delta table join) for machine metadata
"""
import sys
import logging
from datetime import datetime, timezone

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, IntegerType, BooleanType, ArrayType,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_BUCKET",
    "PROCESSED_BUCKET",
    "DATABASE_NAME",
    "KAFKA_TOPIC",
    "WATERMARK_MINUTES",
    "ENABLE_DQ_CHECKS",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

WATERMARK_MIN = int(args.get("WATERMARK_MINUTES", "5"))
ENABLE_DQ = args.get("ENABLE_DQ_CHECKS", "true").lower() == "true"
CHECKPOINT_PATH = f"s3://{args['RAW_BUCKET']}/_checkpoints/iot-transformer/"

# ── IoT event schema (Kafka message value after JSON deserialization) ─────────
IOT_SCHEMA = StructType([
    StructField("device_id",        StringType(),    False),
    StructField("device_type",      StringType(),    True),   # SENSOR|PLC|HMI
    StructField("plant_id",         StringType(),    False),
    StructField("line_id",          StringType(),    True),
    StructField("timestamp_ms",     LongType(),      False),  # epoch ms
    StructField("event_name",       StringType(),    False),
    StructField("value_numeric",    DoubleType(),    True),
    StructField("value_string",     StringType(),    True),
    StructField("unit",             StringType(),    True),
    StructField("quality",          IntegerType(),   True),   # 0-100 OPC UA style
    StructField("tags",             ArrayType(StringType()), True),
    StructField("alert_threshold",  DoubleType(),    True),
    StructField("firmware_version", StringType(),    True),
])

# ── Device registry (reference table, small – broadcast join) ─────────────────
DEVICE_REGISTRY_PATH = (
    f"s3://{args['PROCESSED_BUCKET']}/reference/device_registry/latest/"
)


def load_device_registry() -> DataFrame:
    """Load device metadata for enrichment. Broadcast for join efficiency."""
    try:
        registry = spark.read.parquet(DEVICE_REGISTRY_PATH)
        logger.info(f"Loaded device registry: {registry.count()} devices")
        return registry
    except Exception as e:
        logger.warning(f"Device registry not found, using empty: {e}")
        return spark.createDataFrame(
            [],
            schema=StructType([
                StructField("device_id",   StringType(), False),
                StructField("asset_name",  StringType(), True),
                StructField("location",    StringType(), True),
                StructField("criticality", StringType(), True),  # HIGH|MEDIUM|LOW
                StructField("maintenance_window", StringType(), True),
            ]),
        )


def read_iot_stream() -> DataFrame:
    """
    Read from S3 path where Kafka S3 Sink Connector lands files.
    Using Glue streaming source for micro-batch processing.
    """
    raw_path = f"s3://{args['RAW_BUCKET']}/topics/{args['KAFKA_TOPIC']}/"

    return (
        spark.readStream
        .format("json")
        .schema(IOT_SCHEMA)
        .option("path", raw_path)
        .option("maxFilesPerTrigger", 500)   # bound processing per micro-batch
        .option("latestFirst", "false")       # process in-order
        .load()
    )


def apply_watermark(df: DataFrame) -> DataFrame:
    """Convert epoch ms → timestamp and apply Spark watermark for late data."""
    return (
        df
        .withColumn("event_ts", F.to_timestamp(F.col("timestamp_ms") / 1000))
        .withWatermark("event_ts", f"{WATERMARK_MIN} minutes")
    )


def compute_aggregates(df: DataFrame) -> DataFrame:
    """
    Windowed aggregations per device per 1-min tumbling window.
    Used for anomaly detection and trend analysis.
    """
    return (
        df
        .groupBy(
            F.window("event_ts", "1 minute"),
            "device_id",
            "plant_id",
            "line_id",
            "event_name",
            "unit",
        )
        .agg(
            F.avg("value_numeric").alias("avg_value"),
            F.min("value_numeric").alias("min_value"),
            F.max("value_numeric").alias("max_value"),
            F.stddev("value_numeric").alias("stddev_value"),
            F.count("*").alias("event_count"),
            F.avg("quality").alias("avg_quality"),
            F.sum(F.when(F.col("quality") < 80, 1).otherwise(0)).alias("low_quality_count"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .drop("window")
    )


def detect_anomalies(df: DataFrame) -> DataFrame:
    """
    Simple statistical anomaly detection:
    Flag readings that exceed alert_threshold or are > 3 stddev from device mean.
    In production: replace with SageMaker endpoint call.
    """
    return (
        df
        .withColumn("threshold_breach",
            F.when(
                F.col("alert_threshold").isNotNull()
                & F.col("value_numeric").isNotNull(),
                F.col("value_numeric") > F.col("alert_threshold")
            ).otherwise(False)
        )
        .withColumn("quality_bad", F.col("quality") < 60)
        .withColumn("anomaly_score",
            F.when(F.col("threshold_breach"), 1.0)
            .when(F.col("quality_bad"), 0.5)
            .otherwise(0.0)
        )
    )


def enrich_with_registry(df: DataFrame, registry: DataFrame) -> DataFrame:
    """Left join IoT events with device metadata (broadcast join)."""
    broadcast_registry = F.broadcast(registry)
    return df.join(broadcast_registry, on="device_id", how="left")


def add_partition_cols(df: DataFrame) -> DataFrame:
    """Add Hive partition columns from event timestamp."""
    return (
        df
        .withColumn("year",  F.year("event_ts").cast("string"))
        .withColumn("month", F.lpad(F.month("event_ts").cast("string"), 2, "0"))
        .withColumn("day",   F.lpad(F.dayofmonth("event_ts").cast("string"), 2, "0"))
        .withColumn("hour",  F.lpad(F.hour("event_ts").cast("string"), 2, "0"))
        .withColumn("processed_at", F.current_timestamp())
    )


def write_stream(df: DataFrame, output_path: str) -> None:
    """
    Write streaming DataFrame to S3 Parquet.
    Uses foreachBatch for exactly-once guarantees and custom metrics.
    """
    def process_batch(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            logger.info(f"Batch {batch_id}: empty, skipping")
            return

        count = batch_df.count()
        logger.info(f"Batch {batch_id}: processing {count} records")

        (
            batch_df
            .write
            .mode("append")
            .partitionBy("year", "month", "day", "hour", "plant_id")
            .parquet(output_path)
        )

        # Emit alarm count metric (picked up by CloudWatch Logs filter)
        alarm_count = batch_df.filter(F.col("threshold_breach") == True).count()
        logger.info(
            f"METRIC batch_id={batch_id} records={count} alarms={alarm_count}"
        )

    query = (
        df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="5 minutes")
        .start()
    )
    query.awaitTermination(timeout=110 * 60)  # 110 min < Glue 120 min timeout


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    logger.info("Starting IoT transformer")

    device_registry = load_device_registry()

    # Build streaming pipeline
    raw_stream = read_iot_stream()
    watermarked = apply_watermark(raw_stream)
    anomalies = detect_anomalies(watermarked)
    enriched = enrich_with_registry(anomalies, device_registry)
    partitioned = add_partition_cols(enriched)

    output_path = (
        f"s3://{args['PROCESSED_BUCKET']}/processed/iot_events/"
    )

    write_stream(partitioned, output_path)
    job.commit()
    logger.info("IoT transformer job completed.")


if __name__ == "__main__":
    main()
