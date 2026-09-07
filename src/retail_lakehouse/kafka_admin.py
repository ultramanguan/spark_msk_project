from __future__ import annotations

from dataclasses import dataclass

from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError


@dataclass(frozen=True)
class TopicSpec:
    name: str = "retail-clickstream"
    partitions: int = 6
    replication_factor: int = 3


class _MSKTokenProvider:
    """Bridges kafka-python's OAUTHBEARER mechanism to MSK IAM auth. MSK IAM isn't a kafka-python-native
    SASL mechanism, so token generation is delegated to aws-msk-iam-sasl-signer-python (AWS's own signer)
    on every refresh -- this is the same approach the EC2 instance profile supplies ambiently everywhere
    else on this cluster, just wired through kafka-python's token-provider interface."""

    def __init__(self, region: str) -> None:
        self._region = region

    def token(self) -> str:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

        auth_token, _ = MSKAuthTokenProvider.generate_auth_token(region=self._region)
        return auth_token


def create_topics(bootstrap_servers: str, topics: list[TopicSpec] | None = None,
                   region: str = "us-east-1") -> None:
    """Create Kafka topics on an IAM-authenticated MSK cluster. Idempotent -- topics that already exist
    are skipped rather than raising, so this is safe to call on every pipeline setup/deploy run."""
    specs = topics or [TopicSpec()]
    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=_MSKTokenProvider(region),
        client_id="retail-lakehouse-admin",
    )
    try:
        admin.create_topics([
            NewTopic(name=spec.name, num_partitions=spec.partitions,
                     replication_factor=spec.replication_factor)
            for spec in specs
        ])
    except TopicAlreadyExistsError:
        pass
    finally:
        admin.close()
