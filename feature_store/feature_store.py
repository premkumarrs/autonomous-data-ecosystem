import os
import pandas as pd
from datetime import datetime

FEATURE_DIR = "feature_store/data"
os.makedirs(FEATURE_DIR, exist_ok=True)

def save_features(df):
    run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    features = df[["age", "salary", "department"]].copy()
    features_path = f"{FEATURE_DIR}/features_{run_id}.csv"

    features.to_csv(features_path, index=False)
    print(f"🧠 Features saved to Feature Store: {features_path}")

    return features_path
