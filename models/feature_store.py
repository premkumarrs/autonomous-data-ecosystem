import os
import pandas as pd

FEATURE_DIR = "models/features"
os.makedirs(FEATURE_DIR, exist_ok=True)

def save_features(df, run_id):
    features = df[["age", "salary"]]
    path = f"{FEATURE_DIR}/features_{run_id}.csv"
    features.to_csv(path, index=False)
    print(f"Features saved to: {path}")
    return path
