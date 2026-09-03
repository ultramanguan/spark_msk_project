import os

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", "python")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("retail-lakehouse-tests")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()

