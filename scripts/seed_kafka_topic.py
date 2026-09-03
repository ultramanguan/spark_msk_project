#!/usr/bin/env python3
"""Produce synthetic clickstream events to the retail-clickstream MSK topic.

Usage:
    pip install -e .[dev,kafka]
    python scripts/seed_kafka_topic.py --bootstrap-servers <brokers> --auth iam --count 5000
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from retail_lakehouse.generate import synthetic_events, produce_to_kafka


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", required=True, help="MSK bootstrap broker string")
    parser.add_argument("--topic", default="retail-clickstream")
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--auth", choices=["iam", "none"], default="iam",
                         help="'iam' uses AWS_MSK_IAM via aws-msk-iam-auth-python if installed; "
                              "'none' assumes a PLAINTEXT dev broker")
    args = parser.parse_args()

    producer_kwargs = {}
    if args.auth == "iam":
        producer_kwargs.update({
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "OAUTHBEARER",
            "sasl_oauth_token_provider": _msk_iam_token_provider(),
        })
        security_protocol = "SASL_SSL"
    else:
        security_protocol = "PLAINTEXT"

    sent = produce_to_kafka(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        rows=synthetic_events(args.count),
        security_protocol=security_protocol,
        **({k: v for k, v in producer_kwargs.items() if k != "security_protocol"}),
    )
    print(f"Produced {sent} events to topic '{args.topic}'")


def _msk_iam_token_provider():
    """Lazily import the IAM token provider so --auth none works without the extra dependency installed."""
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError as e:
        raise SystemExit(
            "IAM auth requires the 'aws-msk-iam-sasl-signer-python' package: "
            "pip install aws-msk-iam-sasl-signer-python"
        ) from e

    class TokenProvider:
        def token(self):
            token, _ = MSKAuthTokenProvider.generate_auth_token("us-east-1")
            return token

    return TokenProvider()


if __name__ == "__main__":
    main()
