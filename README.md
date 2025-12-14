# Autonomous Data Ecosystem

Autonomous Data Ecosystem is a production-inspired data platform that demonstrates how modern enterprise data systems are designed, operated, and monitored end to end.
The project implements a single, unified pipeline that ingests raw data, validates schema and quality, generates analytical insights, trains machine learning models, stores versioned features, persists results in a data warehouse, and records execution lineage and performance metrics all in a deterministic and repeatable manner.
Rather than focusing only on ETL, this system emphasizes data reliability, observability, governance, and ML reproducibility, which are critical requirements in real-world data platforms.

---

## End-to-End Pipeline Overview

### Data Ingestion
The pipeline begins by loading structured data from CSV sources. Each execution is associated with a runtime execution date, allowing the system to support deterministic re-runs, historical backfills, and date-partitioned ingestion. This mirrors how production data platforms handle daily or scheduled batch jobs.

### Schema Intelligence
After ingestion, the pipeline automatically extracts the dataset schema and compares it against the most recently saved schema version. Any schema changes are detected and logged, and the current schema is persisted as a historical artifact. This enables early detection of schema drift and protects downstream systems from silent breakage.

### Data Quality and Validation
The system performs multiple data quality operations, including duplicate removal, missing value handling using statistical imputation, and outlier detection using IQR-based logic. A quantitative data quality score is generated for each run. In addition, a rule-based validation engine enforces domain-specific quality constraints and logs any violations for auditing purposes.

### Analytics Layer
Once data quality checks pass, the pipeline computes analytical aggregates such as average salary by department and headcount distribution. These analytics represent typical business-facing metrics and are designed to be reusable by downstream reporting or dashboarding layers.

### Machine Learning Pipeline
The pipeline includes an integrated machine learning stage where categorical features are encoded, a Linear Regression model is trained to predict salary, and model performance is evaluated using Mean Absolute Error (MAE). Trained models are persisted to disk, and model metrics are appended to a historical metrics log. Feature datasets are versioned and stored separately to ensure reproducibility between training and inference.

### Streaming Simulation
To emulate real-time ingestion patterns, the system processes the dataset in configurable micro-batches. This simulation demonstrates how the same pipeline logic can be adapted to streaming or near-real-time data flows without requiring external infrastructure.

### SQL Warehouse Integration
Cleaned and enriched data is persisted into a SQLite-based warehouse. Ingestion is idempotent by execution date, ensuring that re-running the pipeline for the same date does not create duplicate records. The warehouse also stores historical KPI metrics to support trend analysis over time.

### Pipeline Lineage Tracking
Each pipeline run records structured lineage information as JSON artifacts, capturing the logical flow of data across pipeline stages. This provides traceability and auditability without relying on external orchestration or visualization tools.

### Performance Monitoring
The system tracks execution time and memory usage for every run, providing basic observability into pipeline performance and resource utilization.

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


