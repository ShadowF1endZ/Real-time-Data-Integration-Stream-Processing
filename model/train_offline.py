# model/train_offline.py
"""
Train RandomForest trên PaySim offline → lưu model.pkl để Spark load + broadcast.
"""

import pandas as pd
import numpy as np
import pickle
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, roc_auc_score
from sklearn.preprocessing import LabelEncoder

FEATURES = [
    "type_enc", "amount", "oldbalanceOrg", "newbalanceOrig",
    "oldbalanceDest", "newbalanceDest",
    "balance_diff_orig", "balance_diff_dest", "amount_ratio",
]

def engineer(df: pd.DataFrame) -> pd.DataFrame:
    le = LabelEncoder()
    df = df.copy()
    df["type_enc"]          = le.fit_transform(df["type"])
    df["balance_diff_orig"] = df["oldbalanceOrg"]  - df["newbalanceOrig"]
    df["balance_diff_dest"] = df["newbalanceDest"] - df["oldbalanceDest"]
    # Tránh chia 0
    df["amount_ratio"]      = df["amount"] / (df["oldbalanceOrg"] + 1e-6)
    return df

def train(csv_path: str):
    df = pd.read_csv(csv_path)
    df = engineer(df)

    X = df[FEATURES]
    y = df["isFraud"]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, stratify=y, random_state=42
    )

    # Class weight để xử lý imbalance (~1.3% fraud trong PaySim)
    clf = RandomForestClassifier(
        n_estimators=200,
        max_depth=12,
        class_weight="balanced",
        n_jobs=-1,
        random_state=42,
    )
    clf.fit(X_train, y_train)

    y_prob = clf.predict_proba(X_test)[:, 1]
    print(classification_report(y_test, (y_prob > 0.5).astype(int)))
    print(f"AUC-ROC: {roc_auc_score(y_test, y_prob):.4f}")

    with open("model/fraud_model.pkl", "wb") as f:
        pickle.dump(clf, f)
    print("Model saved → model/fraud_model.pkl")

if __name__ == "__main__":
    train("PS_20174392719_1491204439457_log.csv")