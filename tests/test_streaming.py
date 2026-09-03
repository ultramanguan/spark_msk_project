from datetime import datetime, timezone

from retail_lakehouse.streaming import windowed_revenue


def test_windowed_revenue_batch_semantics(spark):
    # window()/withWatermark() work identically on a static DataFrame; this exercises the
    # aggregation logic without needing a running streaming query.
    df = spark.createDataFrame([
        (datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc), "electronics", "e1", 20.0, "purchase"),
        (datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc), "electronics", "e2", 5.0, "purchase"),
        (datetime(2026, 1, 1, 0, 1, 0, tzinfo=timezone.utc), "electronics", "e3", 100.0, "view"),
    ], ["event_ts", "category", "event_id", "gross_amount", "event_type"])
    out = windowed_revenue(df, window_duration="5 minutes").collect()
    assert len(out) == 1
    assert out[0]["orders"] == 2
    assert out[0]["revenue"] == 25.0
