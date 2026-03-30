"""
kairoskop.spark.streaming_job
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
KAIROSKOP — Spark Structured Streaming job.

Reads from four Kafka topics simultaneously, applies enrichment and
philosophical metric computation, then writes enriched events to two
sinks:
  1. GCS Data Lake (Parquet, partitioned by event_date)
  2. BigQuery (partitioned + clustered table for analytical queries)

The job runs continuously with a 5-minute micro-batch trigger.
Checkpointing is enabled for fault tolerance and exactly-once semantics.

Usage (inside Spark container):
    spark-submit \\
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
    com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.36.1 \\
      /opt/spark/jobs/streaming_job.py
"""

from __future__ import annotations

import os
from typing import Iterator

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from enrichment import (
    classify_consciousness_level_udf,
    compute_signal_strength_udf,
    detect_medium_dominance_udf,
)
from schemas import ATTENTION_EVENT_SCHEMA

# ─────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP      = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPICS               = "wiki_pageviews,wiki_changes,gdelt_events,arxiv_papers"
GCS_BUCKET           = os.getenv("GCS_BUCKET_NAME", "kairoskop-datalake")
BQ_PROJECT           = os.getenv("GCP_PROJECT_ID", "")
BQ_DATASET           = os.getenv("BQ_DATASET", "kairoskop")
BQ_TABLE             = "attention_events_raw"
CHECKPOINT_BASE      = os.getenv("SPARK_CHECKPOINT_DIR", "/tmp/checkpoints/kairoskop")
TRIGGER_INTERVAL     = os.getenv("SPARK_TRIGGER_INTERVAL", "5 minutes")
GCS_CREDENTIALS_FILE = os.getenv("GOOGLE_APPLICATION_CREDENTIALS",
                                  "/opt/spark/credentials/sa-key.json")


def build_spark_session() -> SparkSession:
    """
    Build a SparkSession configured for GCS and BigQuery connectors.
    Service account credentials are loaded from the mounted file path.
    """
    spark = (
        SparkSession.builder
        .appName("kairoskop-streaming")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # BigQuery connector
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                GCS_CREDENTIALS_FILE)
        # GCS connector
        .config("fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka_stream(spark: SparkSession) -> "pyspark.sql.DataFrame":
    """
    Read from all four Kafka topics as a single unified stream.

    Using `subscribe` (comma-separated list) rather than a pattern
    keeps the topic contract explicit and avoids accidental subscription
    to unrelated topics in the same cluster.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 50_000)   # backpressure cap
        .load()
    )


def parse_and_enrich(raw_df: "pyspark.sql.DataFrame") -> "pyspark.sql.DataFrame":
    """
    Parse Kafka message values (JSON bytes) against the canonical schema,
    then apply enrichment transformations.

    Enrichment steps:
    1. Parse JSON payload into typed columns
    2. Cast event_timestamp to TimestampType
    3. Derive event_date partition key
    4. Apply UDFs for philosophical metrics:
       - consciousness_level (Wilber)
       - medium_dominance (McLuhan)
       - adjusted signal_strength
    5. Add ingested_at wall-clock timestamp
    """
    # Step 1 — Parse JSON
    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str", "topic AS kafka_topic")
        .withColumn("data", F.from_json(F.col("json_str"), ATTENTION_EVENT_SCHEMA))
        .select("data.*", "kafka_topic")
    )

    # Step 2 & 3 — Timestamps and partition key
    enriched = (
        parsed
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("event_timestamp"))
        )
        .withColumn(
            "event_date",
            F.to_date(F.col("event_timestamp"))
        )
        # Step 4 — Philosophical metrics via UDFs
        .withColumn(
            "consciousness_level",
            classify_consciousness_level_udf(F.col("topic_category"), F.col("source"))
        )
        .withColumn(
            "medium_dominance",
            detect_medium_dominance_udf(F.col("medium_type"), F.col("signal_strength"))
        )
        .withColumn(
            "signal_strength",
            compute_signal_strength_udf(
                F.col("signal_strength"),
                F.col("source"),
                F.col("topic_category")
            )
        )
        # Step 5 — Ingestion timestamp
        .withColumn("ingested_at", F.current_timestamp())
        # Drop internal fields not needed in the warehouse
        .drop("kafka_topic", "raw_payload")
    )

    return enriched


def write_to_gcs(df: "pyspark.sql.DataFrame") -> "pyspark.sql.StreamingQuery":
    """
    Write enriched events to GCS as Parquet, partitioned by event_date.

    Using `outputMode("append")` because we never update or delete
    historical records — each micro-batch adds new rows only.
    """
    return (
        df.writeStream
        .format("parquet")
        .option("path", f"gs://{GCS_BUCKET}/attention_events/")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/gcs")
        .partitionBy("event_date")
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def write_to_bigquery(df: "pyspark.sql.DataFrame") -> "pyspark.sql.StreamingQuery":
    """
    Write enriched events to BigQuery using the BigQuery Storage Write API.

    The target table is pre-created by Terraform with date partitioning
    and clustering — the connector honours those settings automatically.
    """
    return (
        df.writeStream
        .format("bigquery")
        .option("table", f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bigquery")
        .option("createDisposition", "CREATE_NEVER")   # table must exist (Terraform)
        .option("partitionField", "event_date")
        .option("clusteredFields", "source,topic_category")
        .option("writeMethod", "storage_write_api")    # high-throughput path
        .outputMode("append")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


def main() -> None:
    spark = build_spark_session()

    raw_stream    = read_kafka_stream(spark)
    enriched_df   = parse_and_enrich(raw_stream)

    gcs_query = write_to_gcs(enriched_df)
    bq_query  = write_to_bigquery(enriched_df)

    print("KAIROSKOP streaming job running.")
    print(f"  GCS sink:       gs://{GCS_BUCKET}/attention_events/")
    print(f"  BigQuery sink:  {BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}")
    print(f"  Trigger:        {TRIGGER_INTERVAL}")

    # Block until both queries terminate (or a failure occurs)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
