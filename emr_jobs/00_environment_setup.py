"""EMR Step: environment setup — create the Glue database and seed source files.

Production port of notebooks/00_environment_setup.py, adapted for spark-submit on EMR
(argparse instead of dbutils widgets, Glue Data Catalog instead of Unity Catalog).
"""
import argparse
import random

from pyspark.sql import SparkSession

from retail_lakehouse.config import PipelineConfig
from retail_lakehouse.generate import synthetic_customers, synthetic_events, synthetic_products


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--schema", default="retail_lakehouse")
    parser.add_argument("--base-path", required=True)
    return parser.parse_args()


def main():
    args = parse_args()
    spark = SparkSession.builder.appName("00_environment_setup").getOrCreate()
    cfg = PipelineConfig(schema=args.schema, base_path=args.base_path)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{cfg.schema}` LOCATION '{cfg.path('tables')}'")
    spark.sql(f"USE `{cfg.schema}`")
    spark.conf.set("spark.sql.shuffle.partitions", "8")
    print("Spark version:", spark.version)

    random.seed(42)
    product_rows = synthetic_products(50)
    customer_rows = synthetic_customers(200)
    event_rows = list(synthetic_events(2000))

    product_df = spark.createDataFrame(product_rows)
    customer_df = spark.createDataFrame(customer_rows)
    event_df = spark.createDataFrame(event_rows)

    product_df.write.mode("overwrite").format("delta").saveAsTable(cfg.table("dim_product_seed"))
    customer_df.write.mode("overwrite").format("delta").saveAsTable(cfg.table("dim_customer_seed"))
    event_df.write.mode("overwrite").json(cfg.path("source", "events_json"))
    product_df.write.mode("overwrite").option("header", True).csv(cfg.path("source", "products_csv"))
    customer_df.write.mode("overwrite").option("header", True).csv(cfg.path("source", "customers_csv"))

    print("Created seed tables and files")
    spark.sql(f"SHOW TABLES IN `{cfg.schema}`").show()


if __name__ == "__main__":
    main()
