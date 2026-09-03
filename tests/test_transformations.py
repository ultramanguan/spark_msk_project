from datetime import datetime
from retail_lakehouse.transformations import normalize_click_events, filter_valid_events, revenue_by_hour, deduplicate_events


def test_normalize_click_events(spark):
    df = spark.createDataFrame([
        ("e1", "2026-01-01T00:00:00", "u1", "p1", "purchase", 2, 10.0),
        ("e2", "2026-01-01T00:05:00", "u2", "p1", "view", None, None),
    ], ["event_id", "event_ts", "user_id", "product_id", "event_type", "quantity", "price"])
    out = normalize_click_events(df)
    rows = {r["event_id"]: r for r in out.collect()}
    assert rows["e1"]["gross_amount"] == 20.0
    assert rows["e1"]["is_purchase"] is True
    assert rows["e2"]["gross_amount"] == 0.0


def test_filter_valid_events(spark):
    df = spark.createDataFrame([
        ("e1", datetime(2026, 1, 1), "u1", "view"),
        (None, datetime(2026, 1, 1), "u2", "view"),
        ("e3", datetime(2026, 1, 1), "u3", "bad_type"),
    ], ["event_id", "event_ts", "user_id", "event_type"])
    assert filter_valid_events(df).count() == 1


def test_revenue_by_hour(spark):
    df = spark.createDataFrame([
        (datetime(2026, 1, 1, 1), "electronics", "west", "e1", 2, 10.0, 20.0, True, "purchase"),
        (datetime(2026, 1, 1, 1), "electronics", "west", "e2", 1, 5.0, 5.0, False, "view"),
    ], ["event_hour", "category", "region", "event_id", "quantity", "price", "gross_amount", "is_purchase", "event_type"])
    out = revenue_by_hour(df).collect()[0]
    assert out["orders"] == 1
    assert out["revenue"] == 20.0


def test_deduplicate_events_keeps_latest_ingest(spark):
    df = spark.createDataFrame([
        ("e1", datetime(2026, 1, 1, 0, 0)),
        ("e1", datetime(2026, 1, 1, 0, 5)),
        ("e2", datetime(2026, 1, 1, 0, 0)),
    ], ["event_id", "_ingest_ts"])
    out = deduplicate_events(df)
    assert out.count() == 2
    kept = out.filter("event_id = 'e1'").collect()[0]
    assert kept["_ingest_ts"] == datetime(2026, 1, 1, 0, 5)
