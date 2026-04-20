# spark/feature_engineering.py
"""
Feature engineering functions — tách riêng để dễ test và reuse.
Import vào streaming_job.py.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

TYPE_MAP = {
    "PAYMENT": 0,
    "TRANSFER": 1,
    "CASH_OUT": 2,
    "DEBIT": 3,
    "CASH_IN": 4,
}

def add_features(df: DataFrame) -> DataFrame:
    """
    Nhận raw event DataFrame, trả về DataFrame với các feature columns.
    """
    type_map_expr = F.create_map(
        [F.lit(k) for pair in TYPE_MAP.items() for k in pair]
    )

    return (
        df
        .withColumn("type_enc",
            type_map_expr[F.col("type")])
        .withColumn("balance_diff_orig",
            F.col("oldbalanceOrg") - F.col("newbalanceOrig"))
        .withColumn("balance_diff_dest",
            F.col("newbalanceDest") - F.col("oldbalanceDest"))
        .withColumn("amount_ratio",
            F.col("amount") / (F.col("oldbalanceOrg") + 1e-6))
        # Balance về 0 sau giao dịch — dấu hiệu fraud phổ biến trong PaySim
        .withColumn("orig_balance_zero",
            (F.col("newbalanceOrig") == 0).cast(IntegerType()))
        .withColumn("dest_balance_zero",
            (F.col("newbalanceDest") == 0).cast(IntegerType()))
    )