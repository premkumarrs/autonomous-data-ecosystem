# Autonomous Data Ecosystem

Autonomous Data Ecosystem is a **production-inspired end-to-end data platform** that simulates how modern enterprise data systems ingest, validate, govern, analyze, and model data while maintaining strong guarantees around **data reliability, observability, reproducibility, and traceability**.

The system implements a **single deterministic pipeline** that processes raw data, validates schema and data quality, generates analytics, trains machine learning models, stores versioned features, persists results into a warehouse, and records execution lineage and performance metrics.

Unlike traditional ETL projects, this system demonstrates **how real-world enterprise data platforms operate across the entire lifecycle of data and machine learning systems**.

---

# Key Highlights

* Built an **end-to-end ETL pipeline processing 1,001 records**
* Implemented **deduplication, missing value imputation, and outlier detection**
* Stored processed data in a **date-partitioned SQLite warehouse**
* Developed an **ML pipeline using Scikit-learn Linear Regression**
* Implemented **feature encoding and model serialization using Joblib**
* Achieved **Mean Absolute Error (MAE): 41,852**
* Implemented **data validation and monitoring across a 7-stage pipeline**
* Improved dataset quality to **~90%**
* Implemented **pipeline lineage tracking and performance monitoring**

---

# System Architecture

The platform executes a multi-stage pipeline that simulates a **modern enterprise data platform architecture**.

```
Raw Data
   │
   ▼
Data Ingestion
   │
   ▼
Schema Intelligence
   │
   ▼
Data Quality Validation
   │
   ▼
Analytics Layer
   │
   ▼
Machine Learning Pipeline
   │
   ▼
Feature Store + Model Storage
   │
   ▼
SQL Data Warehouse
   │
   ▼
Lineage + Monitoring
```

---

# End-to-End Pipeline Overview

## 1. Data Ingestion

Loads structured CSV datasets with a runtime execution date, enabling:

* deterministic reruns
* historical backfills
* date-partitioned ingestion

---

## 2. Schema Intelligence

Automatically extracts and compares schemas across pipeline runs to detect **schema drift** and persist schema history.

This ensures downstream systems remain safe from unexpected structural changes.

---

## 3. Data Quality and Validation

Performs automated data cleaning and validation including:

* duplicate removal
* missing value imputation
* salary outlier detection using IQR
* rule-based validation engine

The system generates a **data quality score** for each pipeline run.

---

## 4. Analytics Layer

Generates reusable business metrics such as:

* departmental salary averages
* employee headcount distribution
* salary insights across departments

---

## 5. Machine Learning Pipeline

Implements a full machine learning workflow:

* feature encoding using `LabelEncoder`
* training a **Linear Regression model**
* evaluating model performance using **MAE**
* serializing trained models using **Joblib**
* storing versioned feature datasets

Model performance achieved:

**Mean Absolute Error (MAE): 41,852**

---

## 6. Streaming Simulation

Processes data in configurable **micro-batches** to simulate real-time ingestion.

This mimics streaming pipelines used in modern data platforms.

---

## 7. SQL Warehouse Integration

Enriched datasets are persisted into a **SQLite warehouse** with:

* idempotent ingestion
* ingestion date partitioning
* historical KPI tracking

---

## 8. Pipeline Lineage Tracking

Each pipeline run records structured lineage metadata including:

* execution date
* dataset size
* model performance
* data quality score

This provides **full traceability across pipeline stages**.

---

## 9. Performance Monitoring

The system captures runtime observability metrics including:

* execution time
* memory usage

These metrics provide insights into pipeline performance and operational behavior.

---

# Outputs and Artifacts

Each pipeline execution produces multiple artifacts:

1. Cleaned datasets stored as CSV files
2. Versioned schema metadata
3. Data quality metrics and validation logs
4. Trained machine learning models
5. Versioned feature snapshots
6. Historical model performance metrics
7. SQLite warehouse tables with analytical and KPI data
8. Execution lineage metadata
9. Pipeline run summary JSON files

---

# Project Structure

```
Autonomous-Data-Ecosystem
│
├── analytics/          # analytical computations
├── artifacts/          # pipeline run summaries
├── configs/            # pipeline configuration
├── dashboard/          # visualization components
├── data/               # raw input datasets
├── feature_store/      # versioned ML features
├── metadata/           # schema tracking
├── models/             # trained ML models
├── monitoring/         # performance metrics
├── pipelines/          # lineage tracking
├── scripts/            # data validation and schema logic
│
├── generate_large_data.py
├── simulate_daily_ingestion.py
├── main.py             # pipeline orchestrator
├── requirements.txt
└── README.md
```

---

# Running the Project

### 1. Create Environment

```
conda create -n ade python=3.10
conda activate ade
```

### 2. Install Dependencies

```
pip install -r requirements.txt
```

### 3. Run the Pipeline

```
python main.py
```

Optional: run with a custom execution date

```
python main.py 2025-11-01
```

---

# Technology Stack

* Python
* Pandas
* Scikit-learn
* SQLite
* Joblib
* YAML
* Streamlit
* Altair
