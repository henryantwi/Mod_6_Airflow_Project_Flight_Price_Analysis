# Airflow Project: Flight Price Analysis (Module Lab 3)

This project implements the end-to-end data pipeline requirements for **Module Lab 3**. It analyzes flight price data for Bangladesh using Apache Airflow, MySQL, and PostgreSQL.

## Objective
**Goal:** Develop an end-to-end data pipeline using Apache Airflow to process and analyze flight price data for Bangladesh.
**Implementation:** This project ingests raw CSV data, validates and transforms it, computes key performance indicators (KPIs), and stores the final results in a PostgreSQL analytics database.

## Technologies Used
- **Orchestration:** Apache Airflow
- **Staging Database:** MySQL
- **Analytics Database:** PostgreSQL
- **Data Processing:** Python (Pandas)
- **Source:** [Flight Price Dataset of Bangladesh (Kaggle)](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh) (CSV)

## Pipeline Components & Implementation

The Airflow DAG `flight_price_analysis` orchestrates the following four stages, mapping directly to the lab requirements:

### 1. Data Ingestion
**Requirement:** Load CSV data into a staging table in MySQL.
**Implementation:** 
- **Script:** `scripts/data_ingestion.py`
- **Task:** `ingest_csv_to_mysql`
- Reads `Flight_Price_Dataset_of_Bangladesh.csv` from the `data/` directory.
- Standardizes column names and loads raw data into the MySQL table `flight_prices_raw`.

### 2. Data Validation
**Requirement:** Ensure columns exist, handle nulls, validate types, and flag inconsistencies.
**Implementation:**
- **Script:** `scripts/data_validation.py`
- **Task:** `validate_data`
- **Checks Performed:**
  - Verifies existence of columns: `airline`, `source`, `destination`, `base_fare`, `taxes`, `total_fare`.
  - Drops rows with missing critical data (airline/source/dest).
  - Fills missing numeric values with 0.
  - Enforces numeric types and ensures `total_fare >= base_fare`.
  - Enriches data with `is_peak_season` flag based on keywords (e.g., 'Eid', 'Winter') to support seasonal analysis.
- **Output:** Cleaned data stored in MySQL table `flight_prices_validated`.

### 3. Data Transformation & KPI Computation
**Requirement:** Calculate Total Fare and compute specific KPIs (Avg Fare, Seasonal Variation, Booking Counts, Popular Routes).
**Implementation:**
- **Script:** `scripts/data_transformation.py`
- **Task:** `compute_kpis`
- **KPIs Computed:**
  1.  **Average Fare by Airline:** Aggregates base, tax, and total fares per airline.
  2.  **Seasonal Fare Variation:** Compares average fares between Peak and Non-Peak seasons.
  3.  **Booking Count by Airline:** Counts total bookings and breakdowns by class (Economy, Business, etc.).
  4.  **Most Popular Routes:** Identifies top source-destination pairs by booking volume.
- **Output:** KPI tables written directly to the PostgreSQL analytics database.

### 4. Data Loading into PostgreSQL
**Requirement:** Transfer enriched data to PostgreSQL with minimal latency.
**Implementation:**
- **Script:** `scripts/data_loading.py`
- **Task:** `load_to_postgres`
- Transfers the fully validated and enriched dataset from MySQL to the PostgreSQL table `flight_prices` for ad-hoc querying.

---

## How to Run

1.  **Start Services:**
    ```bash
    docker-compose up -d
    ```
    This launches Airflow, MySQL, and PostgreSQL.

2.  **Access Airflow:**
    - URL: http://localhost:8080
    - User/Pass: `admin` / `admin`
    - Trigger the `flight_price_analysis` DAG.

3.  **Check Results:**
    Connect to the PostgreSQL database to view the `flight_prices` table and KPI views.

## Documentation
A comprehensive report covering the pipeline architecture, DAG details, and challenges is available in:
- `docs/pipeline_report.md`