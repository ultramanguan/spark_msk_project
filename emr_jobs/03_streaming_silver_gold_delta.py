"""EMR Step: streaming silver/gold with watermarking, de-dup, and idempotent upsert."""
import argparse

from pyspark.sql import SparkSession

from retail_lakehouse.config import PipelineConfig
from retail_lakehouse.streaming import deduplicate_stream, make_gold_upsert, windowed_revenue
from retail_lakehouse.transformations import enrich_with_product_customer, normalize_click_events


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="retail_lakehouse")
    parser.add_argument("--base-path", required=True)
    parser.add_argument("--bronze-table", default="bronze_clickstream_kafka")
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("03_streaming_silver_gold_delta").getOrCreate()
    cfg = PipelineConfig(schema=args.schema, base_path=args.base_path)
    spark.sql(f"USE `{cfg.schema}`")

    if not spark.catalog.tableExists(f"{cfg.schema}.{args.bronze_table}"):
        raise ValueError(
            f"{args.bronze_table} does not exist yet. Run the Kafka ingest step (or the "
            "file/rate fallback step and pass --bronze-table bronze_clickstream_rate) first."
        )

    bronze_stream = spark.readStream.table(cfg.table(args.bronze_table))
    deduped_stream = deduplicate_stream(bronze_stream, watermark_col="event_ts", watermark_delay="10 minutes")
    normalized_stream = normalize_click_events(deduped_stream)

    products = spark.table(cfg.table("dim_product_seed"))
    customers = spark.table(cfg.table("dim_customer_seed"))
    silver_stream = enrich_with_product_customer(normalized_stream, products, customers)

    silver_query = (
        silver_stream.writeStream
        .format("delta")
        .option("checkpointLocation", cfg.checkpoint("silver_clickstream_streaming"))
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable(cfg.table("silver_clickstream_streaming"))
    )
    silver_query.awaitTermination()
    print("Silver rows:", spark.table(cfg.table("silver_clickstream_streaming")).count())

    gold_table = cfg.table("gold_revenue_windows_streaming")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {gold_table} (
      window STRUCT<start: TIMESTAMP, end: TIMESTAMP>,
      category STRING,
      orders LONG,
      revenue DOUBLE
    ) USING DELTA
    """)

    silver_for_gold = (
        spark.readStream.table(cfg.table("silver_clickstream_streaming"))
        .withWatermark("event_ts", "10 minutes")
    )
    windowed = windowed_revenue(silver_for_gold, window_duration="5 minutes", watermark_delay="10 minutes")

    upsert_fn = make_gold_upsert(
        target_table=gold_table,
        merge_keys=["window", "category"],
        update_cols=["orders", "revenue"],
    )

    gold_query = (
        windowed.writeStream
        .foreachBatch(upsert_fn)
        .option("checkpointLocation", cfg.checkpoint("gold_revenue_windows_streaming"))
        .trigger(availableNow=True)
        .start()
    )
    gold_query.awaitTermination()
    print("Streaming silver/gold pipeline complete")


if __name__ == "__main__":
    main()
