from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

YES_VALUES = {"yes", "y", "true", "1"}
NO_VALUES = {"no", "n", "false", "0"}


def trim_strings(df: DataFrame) -> DataFrame:
    """Trim all string columns."""
    out = df
    for f in df.schema.fields:
        if isinstance(f.dataType, T.StringType):
            out = out.withColumn(f.name, F.trim(F.col(f.name)))
    return out


def normalize_yes_no(df: DataFrame, cols: list[str]) -> DataFrame:
    """Convert 'yes/no' like tokens to booleans; unknown -> None."""

    def yn_to_bool(col):
        c = F.lower(F.col(col))
        return (
            F.when(c.isin(list(YES_VALUES)), F.lit(True))
            .when(c.isin(list(NO_VALUES)), F.lit(False))
            .otherwise(F.lit(None).cast("boolean"))
        )

    out = df
    for c in cols:
        if c in out.columns:
            out = out.withColumn(c, yn_to_bool(c))
    return out


def cast_numerics(df: DataFrame, numeric_cols: list[str]) -> DataFrame:
    """Cast selected columns to DoubleType if present."""
    out = df
    for c in numeric_cols:
        if c in out.columns:
            out = out.withColumn(c, F.col(c).cast(T.DoubleType()))
    return out


def add_bmi(df: DataFrame, height_col: str = "Height", weight_col: str = "Weight") -> DataFrame:
    """Add BMI = Weight / Height^2 when both columns exist."""
    if height_col in df.columns and weight_col in df.columns:
        return df.withColumn("BMI", F.col(weight_col) / (F.col(height_col) ** 2))
    return df
