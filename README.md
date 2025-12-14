# Autonomous Data Ecosystem

Autonomous Data Ecosystem is a production-inspired data platform that demonstrates how modern enterprise data systems are designed, operated, and monitored end to end.
The project implements a single, unified pipeline that ingests raw data, validates schema and quality, generates analytical insights, trains machine learning models, stores versioned features, persists results in a data warehouse, and records execution lineage and performance metrics all in a deterministic and repeatable manner.
Rather than focusing only on ETL, this system emphasizes data reliability, observability, governance, and ML reproducibility, which are critical requirements in real-world data platforms.

---

## End-to-End Pipeline Overview

**> Data Ingestion**  
Loads structured CSV data with a runtime execution date, enabling deterministic re-runs, backfills, and date-partitioned processing.

**> Schema Intelligence**  
Automatically extracts and compares schemas across runs to detect drift and persist schema history for downstream safety.

**> Data Quality and Validation**  
Cleans duplicates, imputes missing values, detects outliers, computes a quality score, and enforces rule-based validations.

**> Analytics Layer**  
Generates reusable business metrics such as departmental salary averages and headcount distributions.

**> Machine Learning Pipeline**  
Encodes features, trains a Linear Regression model, evaluates MAE, stores models, and versions feature datasets for reproducibility.

**> Streaming Simulation**  
Processes data in configurable micro-batches to simulate real-time or near-real-time ingestion patterns.

**> SQL Warehouse Integration**  
Persists enriched data into SQLite with idempotent, date-based ingestion and historical KPI tracking.

**> Pipeline Lineage Tracking**  
Records structured lineage metadata for each run, enabling full traceability across pipeline stages.

**> Performance Monitoring**  
Captures execution time and memory usage to provide basic observability into pipeline performance.

---

## Outputs and Artifacts

Each pipeline execution produces multiple artifacts, including:
- Cleaned datasets stored as CSV files
- Versioned schema metadata
- Data quality metrics and validation logs
- Trained machine learning models
- Versioned feature snapshots
- Historical model performance metrics
- SQL warehouse tables with analytical and KPI data
- Execution lineage and run summary JSON files

---

## How to Run

```bash
conda activate ade
pip install -r requirements.txt
python main.py


