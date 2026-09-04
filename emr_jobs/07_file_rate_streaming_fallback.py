"""EMR Step: file/rate streaming fallback (no MSK required) — same mechanics as the real Kafka ingest."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_lakehouse.config import PipelineConfig
from retail_lakehouse.transformations import add_ingest_metadata


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="retail_lakehouse")
    parser.add_argument("--base-path", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("07_file_rate_streaming_fallback").getOrCreate()
    cfg = PipelineConfig(schema=args.schema, base_path=args.base_path)
    spark.sql(f"USE `{cfg.schema}`")

    stream_events = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 50)
        .option("numPartitions", 2)
        .load()
        .select(
            F.concat(F.lit("rate-"), F.col("value")).alias("event_id"),
            F.col("timestamp").alias("event_ts"),
            F.concat(F.lit("u"), F.lpad((F.col("value") % 200).cast("string"), 5, "0")).alias("user_id"),
            F.concat(F.lit("s"), F.lpad((F.col("value") % 50).cast("string"), 5, "0")).alias("session_id"),
            F.concat(F.lit("p"), F.lpad(((F.col("value") % 50) + 1).cast("string"), 4, "0")).alias("product_id"),
            F.when((F.col("value") % 10) == 0, "purchase").when((F.col("value") % 5) == 0, "add_to_cart").otherwise("view").alias("event_type"),
            F.when((F.col("value") % 10) == 0, 1).otherwise(0).cast("int").alias("quantity"),
            F.when((F.col("value") % 10) == 0, 19.99).otherwise(0.0).cast("double").alias("price"),
            F.lit("product").alias("page"),
            F.lit("rate_source").alias("user_agent"),
        )
    )

    bronze_stream = add_ingest_metadata(stream_events, "rate_source_fallback")

    query = (
        bronze_stream.writeStream
        .format("delta")
        .option("checkpointLocation", cfg.checkpoint("rate_bronze_clickstream"))
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(cfg.table("bronze_clickstream_rate"))
    )
    query.awaitTermination()
    print("AvailableNow fallback stream completed")


if __name__ == "__main__":
    main()
