"""EMR Step: batch medallion pipeline (bronze/silver/gold) from the seed files 00_environment_setup.py produces."""
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_lakehouse.config import PipelineConfig
from retail_lakehouse.transformations import (
    add_ingest_metadata,
    deduplicate_events,
    enrich_with_product_customer,
    filter_valid_events,
    normalize_click_events,
    revenue_by_hour,
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="retail_lakehouse")
    parser.add_argument("--base-path", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("01_batch_lakehouse_bronze_silver_gold").getOrCreate()
    cfg = PipelineConfig(schema=args.schema, base_path=args.base_path)
    spark.sql(f"USE `{cfg.schema}`")

    raw_events = spark.read.json(cfg.path("source", "events_json"))
    products = spark.table(cfg.table("dim_product_seed"))
    customers = spark.table(cfg.table("dim_customer_seed"))
    print("Raw event rows:", raw_events.count())

    bronze = add_ingest_metadata(raw_events, "batch_file_seed")
    bronze.write.mode("overwrite").format("delta").partitionBy("_ingest_date").saveAsTable(cfg.table("bronze_clickstream_batch"))

    silver = deduplicate_events(filter_valid_events(normalize_click_events(bronze)))
    silver.write.mode("overwrite").format("delta").partitionBy("event_date").saveAsTable(cfg.table("silver_clickstream_batch"))
    print("Silver rows:", spark.table(cfg.table("silver_clickstream_batch")).count())

    enriched = enrich_with_product_customer(spark.table(cfg.table("silver_clickstream_batch")), products, customers)
    gold = revenue_by_hour(enriched.withColumn(
        "is_purchase", F.col("event_type").isin("purchase", "checkout")
    ))
    gold.write.mode("overwrite").format("delta").saveAsTable(cfg.table("gold_revenue_by_hour_batch"))

    print("Batch medallion pipeline complete")


if __name__ == "__main__":
    main()
