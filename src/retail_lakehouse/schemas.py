from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

click_event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_ts", TimestampType(), False),
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("event_type", StringType(), False),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("page", StringType(), True),
    StructField("user_agent", StringType(), True),
])

product_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("brand", StringType(), False),
    StructField("price", DoubleType(), False),
])

customer_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("segment", StringType(), False),
    StructField("region", StringType(), False),
    StructField("signup_epoch", LongType(), False),
])
