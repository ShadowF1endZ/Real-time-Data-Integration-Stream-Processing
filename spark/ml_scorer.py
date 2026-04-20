# spark/ml_scorer.py
"""
Load trained model, broadcast đến executors, expose scoring UDF.
Import vào streaming_job.py.
"""

import pickle
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

FEATURE_COLS = [
    "type_enc", "amount", "oldbalanceOrg", "newbalanceOrig",
    "oldbalanceDest", "newbalanceDest",
    "balance_diff_orig", "balance_diff_dest", "amount_ratio",
]

def load_scorer(spark: SparkSession, model_path: str):
    """
    Load model từ disk, broadcast lên tất cả executors,
    trả về pandas_udf để dùng trong Spark job.
    """
    with open(model_path, "rb") as f:
        _model = pickle.load(f)

    model_broadcast = spark.sparkContext.broadcast(_model)

    @pandas_udf(DoubleType())
    def score_fraud(*cols) -> pd.Series:
        X = pd.concat(cols, axis=1)
        X.columns = FEATURE_COLS
        model = model_broadcast.value
        proba = model.predict_proba(X.fillna(0))[:, 1]
        return pd.Series(proba)

    return score_fraud, FEATURE_COLS