import json
import os
from datetime import datetime

SCHEMA_DIR = "metadata/schemas"
os.makedirs(SCHEMA_DIR, exist_ok=True)

def extract_schema(df):
    return {col: str(dtype) for col, dtype in df.dtypes.items()}

def load_latest_schema():
    files = sorted(os.listdir(SCHEMA_DIR))
    if not files:
        return None
    with open(os.path.join(SCHEMA_DIR, files[-1]), "r") as f:
        return json.load(f)

def save_schema(schema):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(SCHEMA_DIR, f"schema_{timestamp}.json")
    with open(path, "w") as f:
        json.dump(schema, f, indent=4)
    print(f"Schema saved: {path}")

def compare_schema(old, new):
    issues = []
    if not old:
        return issues

    for col in new:
        if col not in old:
            issues.append(f"New column detected: {col}")

    for col in old:
        if col not in new:
            issues.append(f"Missing column detected: {col}")

    for col in new:
        if col in old and new[col] != old[col]:
            issues.append(f"Type change in {col}: {old[col]} → {new[col]}")

    return issues
