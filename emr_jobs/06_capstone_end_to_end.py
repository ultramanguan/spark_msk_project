"""EMR Step: capstone end-to-end validation — fails the pipeline (non-zero exit) on a bad result."""
import argparse
import random
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from retail_lakehouse.config import PipelineConfig
from retail_lakehouse.generate import synthetic_customers, synthetic_events, synthetic_products
from retail_lakehouse.quality import assert_no_duplicate_keys
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
    spark = SparkSession.builder.appName("06_capstone_end_to_end").getOrCreate()
    cfg = PipelineConfig(schema=args.schema, base_path=args.base_path)
    spark.sql(f"USE `{cfg.schema}`")
    random.seed(11)

    products = spark.createDataFrame(synthetic_products(50))
    customers = spark.createDataFrame(synthetic_customers(200))
    events = spark.createDataFrame(list(synthetic_events(5000)))

    products.write.mode("overwrite").format("delta").saveAsTable(cfg.table("dim_product_seed"))
    customers.write.mode("overwrite").format("delta").saveAsTable(cfg.table("dim_customer_seed"))

    bronze = add_ingest_metadata(events, "capstone_batch")
    silver = deduplicate_events(filter_valid_events(normalize_click_events(bronze)))
    silver.write.mode("overwrite").format("delta").partitionBy("event_date").saveAsTable(cfg.table("capstone_silver_events"))

    enriched = enrich_with_product_customer(silver, products, customers)
    gold = revenue_by_hour(enriched.withColumn("is_purchase", F.col("event_type").isin("purchase", "checkout")))
    gold.write.mode("overwrite").format("delta").saveAsTable(cfg.table("capstone_gold_revenue"))

    try:
        assert spark.table(cfg.table("capstone_silver_events")).count() > 0, "silver produced no rows"
        assert spark.table(cfg.table("capstone_gold_revenue")).count() > 0, "gold produced no rows"
        assert_no_duplicate_keys(spark.table(cfg.table("capstone_silver_events")), ["event_id"])
    except (AssertionError, ValueError) as exc:
        print(f"Capstone validation FAILED: {exc}", file=sys.stderr)
        sys.exit(1)

    print("Capstone pipeline validated successfully.")


if __name__ == "__main__":
    main()
