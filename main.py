import yaml
import pandas as pd
import os
import json
import time
import psutil
import sqlite3
from datetime import datetime
import sys

from sklearn.preprocessing import LabelEncoder
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error
import joblib

from scripts.data_quality_rules import apply_quality_rules, log_rule_violations
from scripts.schema_intelligence import (
    extract_schema,
    load_latest_schema,
    save_schema,
    compare_schema
)


RUN_DATE = sys.argv[1] if len(sys.argv) > 1 else datetime.now().strftime("%Y-%m-%d")

process = psutil.Process(os.getpid())
start_time = time.time()
start_memory = process.memory_info().rss / (1024 * 1024)

def load_config():
    path = os.path.join("configs", "pipeline.yaml")
    with open(path, "r") as f:
        return yaml.safe_load(f)

def load_data():
    print("\n========== RAW DATA ==========")
    df = pd.read_csv("data/large_sample_data.csv")
    print(df)
    return df

def run_schema_intelligence(df):
    print("\n========== SCHEMA INTELLIGENCE ==========")
    current = extract_schema(df)
    previous = load_latest_schema()
    issues = compare_schema(previous, current)

    if issues:
        for i in issues:
            print("-", i)
    else:
        print("No schema changes detected.")

    save_schema(current)

def clean_and_score(df):
    duplicates = df.duplicated().sum()
    df = df.drop_duplicates()

    df["age"] = df["age"].fillna(df["age"].median())
    df["salary"] = df["salary"].fillna(df["salary"].median())

    q1, q3 = df["salary"].quantile([0.25, 0.75])
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr

    df["salary_outlier"] = (df["salary"] < lower) | (df["salary"] > upper)

    score = max(0, 100 - duplicates * 5 - int(df["salary_outlier"].sum()) * 10)

    print("\n========== CLEANED DATA ==========")
    print(df)

    print("\n========== DATA QUALITY REPORT ==========")
    print(f"Duplicates removed: {duplicates}")
    print(f"Salary outliers detected: {df['salary_outlier'].sum()}")
    print(f"Data Quality Score: {score}/100")

    return df, score, duplicates


def run_rule_engine(df):
    print("\n========== DATA QUALITY RULE ENGINE ==========")
    violations = apply_quality_rules(df)

    if violations:
        for v in violations:
            print(f"- {v['rule']} | {v['count']} rows")
        log_rule_violations(violations)
    else:
        print("All data passed quality rules.")

def run_analytics(df):
    print("\n========== ANALYTICS ENGINE ==========")
    print(df.groupby("department")["salary"].mean())

    print("\nHeadcount:")
    print(df["department"].value_counts())

def run_ml(df):
    print("\n========== ML PIPELINE ==========")

    os.makedirs("feature_store", exist_ok=True)
    os.makedirs("models", exist_ok=True)

    le = LabelEncoder()
    df["department_encoded"] = le.fit_transform(df["department"])

    feature_version = RUN_DATE.replace("-", "")
    df[["age", "department_encoded", "salary"]].to_csv(
        f"feature_store/features_{feature_version}.csv",
        index=False
    )

    X = df[["age", "department_encoded"]]
    y = df["salary"]

    model = LinearRegression()
    model.fit(X, y)

    mae = mean_absolute_error(y, model.predict(X))
    print(f"MAE: {mae:.2f}")

    joblib.dump(model, "models/salary_model.joblib")

    with open("models/metrics.json", "a") as f:
        f.write(json.dumps({
            "run_date": RUN_DATE,
            "mae": float(mae)
        }) + "\n")

    return float(mae)

def run_streaming(df, batch_size):
    print("\n========== STREAMING MICRO-BATCHES ==========")
    for i in range(0, len(df), batch_size):
        print(f"Processing batch | Rows: {len(df.iloc[i:i+batch_size])}")

def update_warehouse(df):
    print("\n========== SQL WAREHOUSE ==========")

    df = df.copy()
    df["ingestion_date"] = RUN_DATE
    df["dept_partition"] = df["department"]

    conn = sqlite3.connect("warehouse.db")
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS employee_data (
            id INTEGER,
            name TEXT,
            age INTEGER,
            salary REAL,
            department TEXT,
            source TEXT,
            salary_outlier BOOLEAN,
            ingestion_date TEXT,
            dept_partition TEXT
        )
    """)

    cur.execute(
        "DELETE FROM employee_data WHERE ingestion_date = ?",
        (RUN_DATE,)
    )

    df.to_sql("employee_data", conn, if_exists="append", index=False)

    total = cur.execute("SELECT COUNT(*) FROM employee_data").fetchone()[0]
    print(f"Total rows in warehouse: {total}")

    conn.close()

def log_metrics():
    end_time = time.time()
    end_memory = process.memory_info().rss / (1024 * 1024)

    print("\n========== PERFORMANCE METRICS ==========")
    print(f"Execution Time: {end_time - start_time:.2f} seconds")
    print(f"Memory Used: {end_memory - start_memory:.2f} MB")

def run_pipeline():
    config = load_config()

    print("\n🚀 Autonomous Data Ecosystem is starting...\n")

    df = load_data()
    run_schema_intelligence(df)

    df, score, dups = clean_and_score(df)
    run_rule_engine(df)

    run_analytics(df)
    mae = run_ml(df)

    if config["pipeline"]["enable_streaming"]:
        run_streaming(df, config["pipeline"]["batch_size"])

    update_warehouse(df)
    log_metrics()

    conn = sqlite3.connect("warehouse.db")

    pd.DataFrame({
        "run_date": [RUN_DATE],
        "avg_salary": [float(df["salary"].mean())],
        "quality_score": [int(score)],
        "rows": [int(len(df))]
    }).to_sql("kpi_history", conn, if_exists="append", index=False)

    conn.close()

    os.makedirs("artifacts", exist_ok=True)
    with open(f"artifacts/run_summary_{RUN_DATE}.json", "w") as f:
        json.dump({
            "run_date": RUN_DATE,
            "rows": int(len(df)),
            "quality_score": int(score),
            "mae": float(mae)
        }, f, indent=2)
    print("\n========== PIPELINE LINEAGE ==========")
    print("\n✅ PIPELINE COMPLETED SUCCESSFULLY")

    from pipelines.lineage import save_lineage

    save_lineage(RUN_DATE)

if __name__ == "__main__":
    run_pipeline()
