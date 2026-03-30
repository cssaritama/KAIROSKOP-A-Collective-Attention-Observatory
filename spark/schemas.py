"""
kairoskop.spark.schemas
~~~~~~~~~~~~~~~~~~~~~~~~~
PySpark StructType schema for the canonical AttentionEvent envelope.

This mirrors the dataclass definition in kafka/schemas.py but expressed
as a PySpark StructType so that from_json() in the streaming job can
parse Kafka message values without schema inference overhead.
"""

from pyspark.sql.types import (
    BooleanType,
    FloatType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ATTENTION_EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(),    nullable=False),
    StructField("source",           StringType(),    nullable=False),
    StructField("event_timestamp",  StringType(),    nullable=False),  # parsed later
    StructField("topic",            StringType(),    nullable=True),
    StructField("topic_category",   StringType(),    nullable=True),
    StructField("language",         StringType(),    nullable=True),
    StructField("country_code",     StringType(),    nullable=True),
    StructField("signal_strength",  FloatType(),     nullable=True),
    StructField("medium_type",      StringType(),    nullable=True),
    StructField("ingested_at",      StringType(),    nullable=True),
])
