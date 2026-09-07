from unittest.mock import MagicMock, patch

from retail_lakehouse.kafka_admin import TopicSpec, create_topics


@patch("retail_lakehouse.kafka_admin.KafkaAdminClient")
def test_create_topics_uses_default_spec(mock_admin_client):
    admin = MagicMock()
    mock_admin_client.return_value = admin

    create_topics("broker:9098")

    ((new_topics,), _kwargs) = admin.create_topics.call_args
    assert [t.name for t in new_topics] == ["retail-clickstream"]
    assert [t.num_partitions for t in new_topics] == [6]
    assert [t.replication_factor for t in new_topics] == [3]
    admin.close.assert_called_once()


@patch("retail_lakehouse.kafka_admin.KafkaAdminClient")
def test_create_topics_accepts_custom_specs(mock_admin_client):
    admin = MagicMock()
    mock_admin_client.return_value = admin

    create_topics("broker:9098", topics=[TopicSpec(name="orders", partitions=3, replication_factor=2)])

    ((new_topics,), _kwargs) = admin.create_topics.call_args
    assert [t.name for t in new_topics] == ["orders"]


@patch("retail_lakehouse.kafka_admin.KafkaAdminClient")
def test_create_topics_is_idempotent_on_already_exists(mock_admin_client):
    from kafka.errors import TopicAlreadyExistsError

    admin = MagicMock()
    admin.create_topics.side_effect = TopicAlreadyExistsError()
    mock_admin_client.return_value = admin

    create_topics("broker:9098")   # must not raise

    admin.close.assert_called_once()
