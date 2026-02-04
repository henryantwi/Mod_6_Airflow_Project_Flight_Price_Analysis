# Flight Price Analysis Pipeline

An Airflow-based data pipeline that processes flight price data for Bangladesh. Takes raw CSV data, cleans it up in MySQL, and loads analytics into PostgreSQL.

The pipeline includes smart duplicate detection using hashing and conditional branching to skip processing when no new data is available.

## What it does
Processes flight price data through multiple stages - ingestion with duplicate detection, validation, transformation, and loading. Calculates metrics like average fares by airline, seasonal pricing patterns, and popular routes. Sends email notifications on completion.

## Tech Stack
- Apache Airflow for orchestration
- MySQL for staging
- PostgreSQL for analytics
- Python & Pandas for processing
- Dataset from [Kaggle](https://www.kaggle.com/datasets/mahatiratusher/flight-price-dataset-of-bangladesh)

## Architecture

flowchart LR
    CSV["CSV File Source<br/>(Kaggle)"] --> INGEST["Ingestion<br/>(Python/SQLAlchemy)"]
    INGEST --> MYSQL_RAW["MySQL Staging<br/>(flight_prices_raw)"]
    MYSQL_RAW --> VALIDATE["Validation & Cleaning<br/>(Pandas)"]
    VALIDATE --> MYSQL_VAL["MySQL Validated<br/>(flight_prices_validated)"]
    MYSQL_VAL --> TRANSFORM["KPI Transformation<br/>(Pandas)"]
    MYSQL_VAL --> LOAD["PostgreSQL Load<br/>(Analytics)"]
    TRANSFORM --> PG_KPI["PostgreSQL KPIs<br/>(4 KPI Tables)"]
    LOAD --> PG_FINAL["PostgreSQL Data<br/>(flight_prices)"]
    
    subgraph Airflow["Airflow Orchestration"]
        INGEST
        VALIDATE
        TRANSFORM
        LOAD
    end


## Pipeline Flow

The DAG uses branching logic to skip processing if no new data is found:

**1. Ingest CSV → MySQL** (`data_ingestion.py`)
- Uses SHA-256 hashing to detect duplicate records
- Only inserts new records into `flight_prices_raw`
- Returns count of new rows inserted

**2. Check for New Data** (branch)
- If new rows > 0: continues to validation
- If no new data: skips to email notification

**3. Validate Data** (`data_validation.py`)
- Checks required columns and drops invalid rows
- Fills missing fare values with 0
- Adds `is_peak_season` flag for Eid/Winter/Hajj periods

**4. Compute KPIs** (`data_transformation.py`)
- Average fare by airline
- Seasonal price variations
- Booking counts by airline and class
- Top 20 popular routes

**5. Load to PostgreSQL** (`data_loading.py`)
- Moves validated data to PostgreSQL analytics DB

**6. Email Notification**
- Success email if new data was processed
- No-change email if no new records found

## Running the Pipeline

```bash
docker compose up -d
```

Access Airflow at http://localhost:8080 (admin/admin). The DAG is set to manual trigger only (no schedule).

Email notifications require setting `TO_USER_EMAIL_1` in your `.env` file.

Results are in the PostgreSQL analytics database - check the `flight_prices` table and KPI tables.
