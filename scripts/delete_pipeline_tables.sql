-- Replace main.retail_lakehouse with your catalog/schema before running.
DROP TABLE IF EXISTS main.retail_lakehouse.dim_product_seed;
DROP TABLE IF EXISTS main.retail_lakehouse.dim_customer_seed;
DROP TABLE IF EXISTS main.retail_lakehouse.dim_customer_current;
DROP TABLE IF EXISTS main.retail_lakehouse.bronze_clickstream_batch;
DROP TABLE IF EXISTS main.retail_lakehouse.silver_clickstream_batch;
DROP TABLE IF EXISTS main.retail_lakehouse.gold_revenue_by_hour_batch;
DROP TABLE IF EXISTS main.retail_lakehouse.bronze_clickstream_kafka;
DROP TABLE IF EXISTS main.retail_lakehouse.silver_clickstream_streaming;
DROP TABLE IF EXISTS main.retail_lakehouse.gold_revenue_windows_streaming;
DROP TABLE IF EXISTS main.retail_lakehouse.bronze_clickstream_rate;
DROP TABLE IF EXISTS main.retail_lakehouse.iceberg_clickstream_sample;
DROP TABLE IF EXISTS main.retail_lakehouse.delta_iceberg_comparison_sample;
DROP TABLE IF EXISTS main.retail_lakehouse.capstone_silver_events;
DROP TABLE IF EXISTS main.retail_lakehouse.capstone_gold_revenue;
