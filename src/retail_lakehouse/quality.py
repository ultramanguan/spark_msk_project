from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def quality_summary(df: DataFrame) -> DataFrame:
    """Return simple null/validity checks as a DataFrame for dashboarding."""
    total = df.count()
    rows = []
    for col in df.columns:
        nulls = df.filter(F.col(col).isNull()).count()
        rows.append((col, total, nulls, 0 if total == 0 else round(nulls / total, 4)))
    return df.sparkSession.createDataFrame(rows, ["column_name", "row_count", "null_count", "null_ratio"])


def assert_no_duplicate_keys(df: DataFrame, key_cols: list[str]) -> None:
    """Raise if any combination of `key_cols` appears more than once. Intended for CI/notebook assertions
    on silver/gold tables where a key should be unique."""
    dup_count = (
        df.groupBy(*key_cols).count().filter(F.col("count") > 1).count()
    )
    if dup_count > 0:
        raise ValueError(f"Found {dup_count} duplicate key(s) on {key_cols}")
