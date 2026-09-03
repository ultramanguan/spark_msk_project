from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from retail_lakehouse.schemas import click_event_schema


def add_ingest_metadata(df: DataFrame, source: str) -> DataFrame:
    """Add standard ingestion metadata columns used in bronze tables."""
    return (
        df.withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_ingest_date", F.to_date(F.col("_ingest_ts")))
          .withColumn("_source", F.lit(source))
    )


def parse_kafka_value(df: DataFrame) -> DataFrame:
    """Parse a raw Kafka/MSK DataFrame's binary `key`/`value` columns into typed clickstream columns.

    Expects the DataFrame produced by `spark.readStream.format("kafka")...load()`.
    """
    return (
        df.select(
            F.col("topic"), F.col("partition"), F.col("offset"),
            F.col("timestamp").alias("kafka_ts"),
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("json_payload"),
        )
        .withColumn("event", F.from_json(F.col("json_payload"), click_event_schema))
        .select("topic", "partition", "offset", "kafka_ts", "kafka_key", "json_payload", "event.*")
    )


def normalize_click_events(df: DataFrame) -> DataFrame:
    """Normalize clickstream events to a stable silver schema."""
    return (
        df.withColumn("event_ts", F.to_timestamp("event_ts"))
          .withColumn("event_date", F.to_date("event_ts"))
          .withColumn("event_hour", F.date_trunc("hour", F.col("event_ts")))
          .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(0)).cast("int"))
          .withColumn("price", F.coalesce(F.col("price"), F.lit(0.0)).cast("double"))
          .withColumn("gross_amount", F.round(F.col("quantity") * F.col("price"), 2))
          .withColumn("is_purchase", F.col("event_type").isin("purchase", "checkout"))
    )


def filter_valid_events(df: DataFrame) -> DataFrame:
    """Keep only records that satisfy minimum quality rules."""
    return df.filter(
        F.col("event_id").isNotNull()
        & F.col("event_ts").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("event_type").isin("view", "add_to_cart", "purchase", "search", "checkout")
    )


def deduplicate_events(df: DataFrame) -> DataFrame:
    """Keep the latest row per event_id for batch de-duplication."""
    w = Window.partitionBy("event_id").orderBy(F.col("_ingest_ts").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def enrich_with_product_customer(events: DataFrame, products: DataFrame, customers: DataFrame) -> DataFrame:
    return (
        events.alias("e")
        .join(F.broadcast(products).alias("p"), "product_id", "left")
        .join(customers.alias("c"), "user_id", "left")
        .select(
            "e.event_id", "e.event_ts", "e.event_date", "e.event_hour", "e.user_id", "e.session_id",
            "e.product_id", "e.event_type", "e.quantity", "e.price", "e.gross_amount",
            "p.category", "p.brand", "c.segment", "c.region", "e.page", "e.user_agent", "e._source", "e._ingest_ts"
        )
    )


def revenue_by_hour(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("is_purchase") | F.col("event_type").isin("purchase", "checkout"))
          .groupBy("event_hour", "category", "region")
          .agg(
              F.countDistinct("event_id").alias("orders"),
              F.sum("quantity").alias("units"),
              F.round(F.sum("gross_amount"), 2).alias("revenue"),
          )
    )


def event_funnel(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("event_type")
          .agg(F.count("*").alias("events"), F.countDistinct("user_id").alias("users"))
          .orderBy(F.desc("events"))
    )
