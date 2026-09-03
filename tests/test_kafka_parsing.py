import json
from datetime import datetime, timezone

from retail_lakehouse.transformations import parse_kafka_value


def test_parse_kafka_value(spark):
    payload = json.dumps({
        "event_id": "e1", "event_ts": "2026-01-01T00:00:00", "user_id": "u1",
        "session_id": "s1", "product_id": "p1", "event_type": "purchase",
        "quantity": 2, "price": 10.0, "page": "cart", "user_agent": "web",
    })
    df = spark.createDataFrame(
        [("retail-clickstream", 0, 100, datetime(2026, 1, 1, tzinfo=timezone.utc), b"u1", payload.encode())],
        ["topic", "partition", "offset", "timestamp", "key", "value"],
    )
    out = parse_kafka_value(df).collect()[0]
    assert out["event_id"] == "e1"
    assert out["quantity"] == 2
    assert out["kafka_key"] == "u1"
