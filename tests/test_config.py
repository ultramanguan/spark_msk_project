from retail_lakehouse.config import PipelineConfig


def test_table_uses_two_level_glue_naming():
    cfg = PipelineConfig(schema="retail_lakehouse")
    assert cfg.table("bronze_clickstream") == "`retail_lakehouse`.`bronze_clickstream`"


def test_table_uses_three_level_naming_when_catalog_set():
    cfg = PipelineConfig(catalog="main", schema="retail_lakehouse")
    assert cfg.table("bronze_clickstream") == "`main`.`retail_lakehouse`.`bronze_clickstream`"


def test_checkpoint_joins_base_path():
    cfg = PipelineConfig(base_path="s3://my-bucket/data/")
    assert cfg.checkpoint("silver_stream") == "s3://my-bucket/data/checkpoints/silver_stream"


def test_path_strips_slashes():
    cfg = PipelineConfig(base_path="s3://my-bucket/data")
    assert cfg.path("/source/", "/events.jsonl") == "s3://my-bucket/data/source/events.jsonl"


def test_catalog_defaults_to_empty_string():
    cfg = PipelineConfig()
    assert cfg.catalog == ""
