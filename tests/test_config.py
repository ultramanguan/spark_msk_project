import dataclasses

from retail_lakehouse.config import PipelineConfig


def test_table_uses_two_level_glue_naming():
    cfg = PipelineConfig(schema="retail_lakehouse")
    assert cfg.table("bronze_clickstream") == "`retail_lakehouse`.`bronze_clickstream`"


def test_checkpoint_joins_base_path():
    cfg = PipelineConfig(base_path="s3://my-bucket/data/")
    assert cfg.checkpoint("silver_stream") == "s3://my-bucket/data/checkpoints/silver_stream"


def test_path_strips_slashes():
    cfg = PipelineConfig(base_path="s3://my-bucket/data")
    assert cfg.path("/source/", "/events.jsonl") == "s3://my-bucket/data/source/events.jsonl"


def test_no_catalog_field():
    field_names = {f.name for f in dataclasses.fields(PipelineConfig)}
    assert "catalog" not in field_names
