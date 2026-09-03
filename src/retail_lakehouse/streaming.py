from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession


def deduplicate_stream(df: DataFrame, watermark_col: str = "event_ts", watermark_delay: str = "10 minutes",
                        dedup_keys: list[str] | None = None) -> DataFrame:
    """Watermark + de-duplicate a streaming DataFrame. Required before any stateful aggregation
    that should run in `append` output mode.

    Stays on the DataFrame API: watermarking and subset-key de-duplication are stateful streaming
    operators with no Spark SQL equivalent (SQL's `SELECT DISTINCT` only dedups on *all* columns).
    """
    keys = dedup_keys or ["event_id"]
    return df.withWatermark(watermark_col, watermark_delay).dropDuplicates(keys)


def windowed_revenue(df: DataFrame, window_duration: str = "5 minutes",
                      watermark_delay: str = "10 minutes") -> DataFrame:
    """Aggregate purchase events into event-time windows. Requires `df` to already carry a watermark
    (e.g. via `deduplicate_stream`) so `append` output mode is valid."""
    spark = df.sparkSession
    df.createOrReplaceTempView("_windowed_revenue_input")
    return spark.sql(f"""
        SELECT
            window(event_ts, '{window_duration}') AS window,
            category,
            COUNT(DISTINCT event_id) AS orders,
            ROUND(SUM(gross_amount), 2) AS revenue
        FROM _windowed_revenue_input
        WHERE event_type IN ('purchase', 'checkout')
        GROUP BY window(event_ts, '{window_duration}'), category
    """)


def make_gold_upsert(target_table: str, merge_keys: list[str], update_cols: list[str]) -> Callable:
    """Build a `foreachBatch` function that MERGEs a micro-batch's aggregated rows into a gold
    Delta table by `merge_keys`, updating `update_cols` on match and inserting otherwise.

    This is the idempotent-upsert pattern used to make a streaming aggregation safe to replay:
    re-running the same micro-batch after a failure updates the same rows instead of duplicating them.
    """
    merge_condition = " AND ".join(f"t.{k} = s.{k}" for k in merge_keys)
    update_set_sql = ", ".join(f"{c} = s.{c}" for c in update_cols)

    def upsert(batch_df: DataFrame, batch_id: int) -> None:
        spark: SparkSession = batch_df.sparkSession
        batch_df.createOrReplaceTempView("_gold_upsert_source")
        spark.sql(f"""
            MERGE INTO {target_table} AS t
            USING _gold_upsert_source AS s
            ON {merge_condition}
            WHEN MATCHED THEN UPDATE SET {update_set_sql}
            WHEN NOT MATCHED THEN INSERT *
        """)

    return upsert
