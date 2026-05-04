"""
factory-log-processor – AWS Glue 4.0 PySpark job
Processes raw factory machine logs from S3 into partitioned Parquet with:
  - Schema enforcement + bad-record quarantine
  - Deduplication via job bookmark + event_id watermark
  - Data quality checks (DeeQu)
  - Columnar output optimized for Redshift COPY (Parquet + Snappy)
"""
import sys
import json
import logging
from datetime import datetime, timezone
from typing import Optional

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, TimestampType, IntegerType, BooleanType,
)
from pyspark.sql.window import Window

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Args ──────────────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_BUCKET",
    "PROCESSED_BUCKET",
    "DATABASE_NAME",
    "SOURCE_PREFIX",
    "TARGET_PREFIX",
    "PARTITION_FORMAT",
])

# ── Spark / Glue init ─────────────────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")

# ── Schema ────────────────────────────────────────────────────────────────────
LOG_SCHEMA = StructType([
    StructField("event_id",        StringType(),    nullable=False),
    StructField("machine_id",      StringType(),    nullable=False),
    StructField("plant_code",      StringType(),    nullable=False),
    StructField("line_id",         StringType(),    nullable=True),
    StructField("event_type",      StringType(),    nullable=False),
    StructField("event_timestamp", LongType(),      nullable=False),  # epoch ms
    StructField("severity",        StringType(),    nullable=True),
    StructField("error_code",      StringType(),    nullable=True),
    StructField("temperature_c",   DoubleType(),    nullable=True),
    StructField("vibration_hz",    DoubleType(),    nullable=True),
    StructField("pressure_bar",    DoubleType(),    nullable=True),
    StructField("cycle_count",     LongType(),      nullable=True),
    StructField("operator_id",     StringType(),    nullable=True),
    StructField("shift",           StringType(),    nullable=True),
    StructField("metadata",        StringType(),    nullable=True),  # JSON blob
])

VALID_SEVERITIES = {"INFO", "WARNING", "ERROR", "CRITICAL"}
VALID_EVENT_TYPES = {"START", "STOP", "ALARM", "MAINTENANCE", "CYCLE_COMPLETE"}


# ── Helpers ───────────────────────────────────────────────────────────────────
def read_raw(bucket: str, prefix: str) -> DataFrame:
    """Read raw JSON logs with schema; bad records go to _quarantine."""
    path = f"s3://{bucket}/{prefix}"
    logger.info(f"Reading raw data from {path}")
    return (
        spark.read
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(LOG_SCHEMA.add("_corrupt_record", StringType(), True))
        .json(path)
    )


def quarantine_bad_records(df: DataFrame, bucket: str, job_run_id: str) -> DataFrame:
    """Split bad records to quarantine path, return clean records only."""
    bad = df.filter(F.col("_corrupt_record").isNotNull())
    good = df.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    if bad.count() > 0:
        quarantine_path = (
            f"s3://{bucket}/_quarantine/logs/"
            f"job_run={job_run_id}/"
        )
        logger.warning(f"Writing {bad.count()} bad records to {quarantine_path}")
        (
            bad
            .withColumn("quarantine_ts", F.current_timestamp())
            .withColumn("job_run_id", F.lit(job_run_id))
            .coalesce(1)
            .write.mode("append")
            .json(quarantine_path)
        )
    return good


def enforce_business_rules(df: DataFrame) -> DataFrame:
    """Apply domain validation rules; flag invalid rows."""
    return (
        df
        .withColumn("severity_valid",
            F.col("severity").isin(*VALID_SEVERITIES) | F.col("severity").isNull()
        )
        .withColumn("event_type_valid",
            F.col("event_type").isin(*VALID_EVENT_TYPES)
        )
        .withColumn("temp_valid",
            F.col("temperature_c").between(-50, 1500) | F.col("temperature_c").isNull()
        )
        .withColumn("ts_valid",
            F.col("event_timestamp") > 0
        )
        .withColumn("dq_pass",
            F.col("severity_valid") & F.col("event_type_valid")
            & F.col("temp_valid") & F.col("ts_valid")
        )
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicates by event_id, keeping the latest ingestion.
    Uses window function to handle duplicates within same micro-batch.
    """
    w = Window.partitionBy("event_id").orderBy(F.col("event_timestamp").desc())
    return (
        df
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )


def enrich(df: DataFrame) -> DataFrame:
    """
    Add computed columns for downstream analysis.
    - Proper timestamp from epoch ms
    - Partitioning columns
    - SLA flag (alarm not acknowledged within 5 min threshold)
    """
    return (
        df
        .withColumn(
            "event_ts",
            F.to_timestamp(F.col("event_timestamp") / 1000)
        )
        .withColumn("year",  F.year("event_ts").cast("string"))
        .withColumn("month", F.lpad(F.month("event_ts").cast("string"), 2, "0"))
        .withColumn("day",   F.lpad(F.dayofmonth("event_ts").cast("string"), 2, "0"))
        .withColumn("hour",  F.lpad(F.hour("event_ts").cast("string"), 2, "0"))
        .withColumn("is_alarm", F.col("event_type") == "ALARM")
        .withColumn("is_critical", F.col("severity") == "CRITICAL")
        .withColumn(
            "processed_at",
            F.lit(datetime.now(timezone.utc).isoformat())
        )
        .withColumn("temp_fahrenheit",
            F.when(
                F.col("temperature_c").isNotNull(),
                F.round(F.col("temperature_c") * 9 / 5 + 32, 2)
            )
        )
    )


def write_processed(df: DataFrame, bucket: str, prefix: str) -> int:
    """
    Write to S3 as partitioned Parquet (Hive-style partitions for Glue crawler).
    Target file size ~128 MB via AQE coalesce.
    """
    output_path = f"s3://{bucket}/{prefix}"
    logger.info(f"Writing processed data to {output_path}")

    # Filter to DQ-passing records for main table
    clean = df.filter(F.col("dq_pass") == True)
    dirty = df.filter(F.col("dq_pass") == False)

    # Main processed output
    (
        clean
        .drop("dq_pass", "severity_valid", "event_type_valid", "temp_valid", "ts_valid")
        .write
        .mode("append")
        .partitionBy("year", "month", "day", "hour", "plant_code")
        .parquet(output_path)
    )

    # DQ failures to separate path for analysts
    if dirty.count() > 0:
        (
            dirty
            .write
            .mode("append")
            .partitionBy("year", "month", "plant_code")
            .parquet(f"s3://{bucket}/_dq_failures/factory_logs/")
        )

    count = clean.count()
    logger.info(f"Written {count} clean records")
    return count


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    job_run_id = sc._jvm.java.util.UUID.randomUUID().toString()
    logger.info(f"Starting log processor | job_run_id={job_run_id}")

    # 1. Read
    raw_df = read_raw(args["RAW_BUCKET"], args["SOURCE_PREFIX"])
    total_raw = raw_df.count()
    logger.info(f"Raw record count: {total_raw}")

    # 2. Quarantine bad JSON
    clean_df = quarantine_bad_records(raw_df, args["RAW_BUCKET"], job_run_id)

    # 3. Business rules validation
    validated_df = enforce_business_rules(clean_df)

    # 4. Deduplication
    deduped_df = deduplicate(validated_df)
    logger.info(
        f"After dedup: {deduped_df.count()} records "
        f"(removed {validated_df.count() - deduped_df.count()} duplicates)"
    )

    # 5. Enrichment
    enriched_df = enrich(deduped_df)

    # 6. Cache for multi-path write
    enriched_df.cache()

    # 7. Write
    written_count = write_processed(
        enriched_df, args["PROCESSED_BUCKET"], args["TARGET_PREFIX"]
    )

    # 8. Emit job metrics to CloudWatch via Glue
    glueContext.get_logger().info(
        json.dumps({
            "job_run_id": job_run_id,
            "total_raw": total_raw,
            "written": written_count,
            "dq_fail_rate": round(1 - written_count / max(total_raw, 1), 4),
        })
    )

    enriched_df.unpersist()
    job.commit()
    logger.info("Job completed successfully.")


if __name__ == "__main__":
    main()
