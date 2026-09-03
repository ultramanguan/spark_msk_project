from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

EVENT_TYPES = ["view", "add_to_cart", "purchase", "search", "checkout"]
CATEGORIES = ["electronics", "grocery", "home", "apparel", "beauty"]
BRANDS = ["acme", "northstar", "evergreen", "summit", "nova"]
REGIONS = ["west", "central", "south", "east"]
SEGMENTS = ["new", "active", "loyal", "at_risk"]


def synthetic_products(n: int = 50) -> list[dict]:
    return [
        {
            "product_id": f"p{i:04d}",
            "category": random.choice(CATEGORIES),
            "brand": random.choice(BRANDS),
            "price": round(random.uniform(3, 500), 2),
        }
        for i in range(1, n + 1)
    ]


def synthetic_customers(n: int = 200) -> list[dict]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return [
        {
            "user_id": f"u{i:05d}",
            "segment": random.choice(SEGMENTS),
            "region": random.choice(REGIONS),
            "signup_epoch": int((base + timedelta(days=random.randint(0, 600))).timestamp()),
        }
        for i in range(1, n + 1)
    ]


def synthetic_events(n: int = 1000, start: datetime | None = None) -> Iterable[dict]:
    start = start or datetime.now(timezone.utc) - timedelta(hours=1)
    products = [f"p{i:04d}" for i in range(1, 51)]
    users = [f"u{i:05d}" for i in range(1, 201)]
    for i in range(n):
        event_type = random.choice(EVENT_TYPES)
        product_id = random.choice(products)
        quantity = random.randint(1, 5) if event_type in {"purchase", "checkout"} else None
        price = round(random.uniform(3, 500), 2) if event_type in {"purchase", "checkout"} else None
        yield {
            "event_id": f"evt-{i:08d}",
            "event_ts": (start + timedelta(seconds=random.randint(0, 3600))).isoformat(),
            "user_id": random.choice(users),
            "session_id": f"s{random.randint(1, 300):05d}",
            "product_id": product_id,
            "event_type": event_type,
            "quantity": quantity,
            "price": price,
            "page": random.choice(["home", "search", "product", "cart", "checkout"]),
            "user_agent": random.choice(["ios", "android", "web", "bot"]),
        }


def write_jsonl(path: str | Path, rows: Iterable[dict]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")


def produce_to_kafka(bootstrap_servers: str, topic: str, rows: Iterable[dict],
                      security_protocol: str = "PLAINTEXT", **producer_kwargs) -> int:
    """Produce synthetic clickstream events to a Kafka/MSK topic, keyed by user_id so a given
    user's events land in the same partition and stay ordered relative to each other.

    Requires the `kafka-python` package (see the `kafka` extra in pyproject.toml). Not used by
    Spark itself -- this is a standalone producer for seeding a demo topic, e.g. from a laptop or
    a small EC2/Lambda job, separate from the Spark Structured Streaming consumer side.
    """
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        security_protocol=security_protocol,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        **producer_kwargs,
    )
    sent = 0
    try:
        for row in rows:
            producer.send(topic, key=row["user_id"], value=row)
            sent += 1
        producer.flush()
    finally:
        producer.close()
    return sent
