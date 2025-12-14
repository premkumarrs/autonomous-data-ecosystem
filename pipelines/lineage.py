import json
import os

def save_lineage(run_date):
    os.makedirs("artifacts", exist_ok=True)

    lineage = {
        "run_date": run_date,
        "pipeline": [
            "Data Ingestion",
            "Schema Intelligence",
            "Data Quality",
            "Analytics",
            "ML Pipeline",
            "Feature Store",
            "SQL Warehouse"
        ],
        "edges": [
            ["Ingestion", "Schema"],
            ["Schema", "Quality"],
            ["Quality", "Analytics"],
            ["Analytics", "ML"],
            ["ML", "Feature Store"],
            ["Feature Store", "Warehouse"]
        ]
    }

    with open(f"artifacts/lineage_{run_date}.json", "w") as f:
        json.dump(lineage, f, indent=2)

    print("Lineage saved → artifacts/lineage_" + run_date + ".json")
