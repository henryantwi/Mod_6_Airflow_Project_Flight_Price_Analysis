# Lab 3: Airflow Flight Price Analysis - Hands-On Steps

This guide provides step-by-step instructions for building an end-to-end data pipeline using Apache Airflow.

---

## 📋 Prerequisites

Before starting, ensure you have:
- [ ] Docker Desktop installed and running
- [ ] Python 3.8+ installed
- [ ] The dataset (`Flight_Price_Dataset_of_Bangladesh.csv`) in the `data/` folder ✅

---

## Step 1: Set Up Docker Environment

### 1.1 Create the Docker Compose File

Create `docker-compose.yml` in your project root:

```yaml
version: '3.8'

x-airflow-common: &airflow-common
  image: apache/airflow:2.7.0
  environment:
    - AIRFLOW__CORE__EXECUTOR=LocalExecutor
    - AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres_airflow:5432/airflow
    - AIRFLOW__CORE__FERNET_KEY=FB0o_zt4e3Ziq3LdUUO7F2Z95cvFFx16hU8jTeR1ASM=
    - AIRFLOW__CORE__LOAD_EXAMPLES=False
    - AIRFLOW__WEBSERVER__SECRET_KEY=your_secret_key_here
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
    - ./scripts:/opt/airflow/scripts
    - ./data:/opt/airflow/data
  depends_on:
    - postgres_airflow
    - mysql
    - postgres_analytics

services:
  # Airflow's metadata database
  postgres_airflow:
    image: postgres:13
    environment:
      - POSTGRES_USER=airflow
      - POSTGRES_PASSWORD=airflow
      - POSTGRES_DB=airflow
    volumes:
      - postgres_airflow_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 5s
      retries: 5

  # Staging Database (MySQL)
  mysql:
    image: mysql:8.0
    environment:
      - MYSQL_ROOT_PASSWORD=rootpassword
      - MYSQL_DATABASE=staging
      - MYSQL_USER=staging_user
      - MYSQL_PASSWORD=staging_password
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
      - ./sql/mysql_schema.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 5s
      retries: 5

  # Analytics Database (PostgreSQL)
  postgres_analytics:
    image: postgres:13
    environment:
      - POSTGRES_USER=analytics_user
      - POSTGRES_PASSWORD=analytics_password
      - POSTGRES_DB=analytics
    ports:
      - "5433:5432"
    volumes:
      - postgres_analytics_data:/var/lib/postgresql/data
      - ./sql/postgres_schema.sql:/docker-entrypoint-initdb.d/init.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U analytics_user -d analytics"]
      interval: 5s
      retries: 5

  # Airflow Webserver
  airflow-webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
      interval: 10s
      timeout: 10s
      retries: 5

  # Airflow Scheduler
  airflow-scheduler:
    <<: *airflow-common
    command: scheduler

  # Airflow Init (one-time setup)
  airflow-init:
    <<: *airflow-common
    entrypoint: /bin/bash
    command: >
      -c "
        airflow db init &&
        airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
      "

volumes:
  postgres_airflow_data:
  mysql_data:
  postgres_analytics_data:
```

### 1.2 Start the Docker Containers

```bash
# Start all services
docker-compose up -d

# Wait for services to be healthy (about 30-60 seconds)
docker-compose ps

# Check logs if needed
docker-compose logs -f airflow-webserver
```

### 1.3 Access Airflow UI

Open your browser and go to: **http://localhost:8080**
- Username: `admin`
- Password: `admin`

---

## Step 2: Create Database Schemas

### 2.1 MySQL Staging Schema

Create `sql/mysql_schema.sql`:

```sql
-- MySQL Staging Database Schema
USE staging;

-- Raw flight prices table (matches CSV structure)
CREATE TABLE IF NOT EXISTS flight_prices_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source VARCHAR(10),
    source_name VARCHAR(200),
    destination VARCHAR(10),
    destination_name VARCHAR(200),
    departure_datetime DATETIME,
    arrival_datetime DATETIME,
    duration_hrs DECIMAL(10, 4),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(50),
    class VARCHAR(50),
    booking_source VARCHAR(50),
    base_fare_bdt DECIMAL(15, 2),
    tax_surcharge_bdt DECIMAL(15, 2),
    total_fare_bdt DECIMAL(15, 2),
    seasonality VARCHAR(50),
    days_before_departure INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Validated flight prices table
CREATE TABLE IF NOT EXISTS flight_prices_validated (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    source VARCHAR(10) NOT NULL,
    source_name VARCHAR(200),
    destination VARCHAR(10) NOT NULL,
    destination_name VARCHAR(200),
    departure_datetime DATETIME,
    arrival_datetime DATETIME,
    duration_hrs DECIMAL(10, 4),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(50),
    class VARCHAR(50),
    booking_source VARCHAR(50),
    base_fare_bdt DECIMAL(15, 2) NOT NULL,
    tax_surcharge_bdt DECIMAL(15, 2) NOT NULL,
    total_fare_bdt DECIMAL(15, 2) NOT NULL,
    seasonality VARCHAR(50),
    days_before_departure INT,
    is_peak_season BOOLEAN DEFAULT FALSE,
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 2.2 PostgreSQL Analytics Schema

Create `sql/postgres_schema.sql`:

```sql
-- PostgreSQL Analytics Database Schema

-- Processed flight data
CREATE TABLE IF NOT EXISTS flight_prices (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    source VARCHAR(10) NOT NULL,
    source_name VARCHAR(200),
    destination VARCHAR(10) NOT NULL,
    destination_name VARCHAR(200),
    departure_datetime TIMESTAMP,
    arrival_datetime TIMESTAMP,
    duration_hrs DECIMAL(10, 4),
    stopovers VARCHAR(50),
    aircraft_type VARCHAR(50),
    class VARCHAR(50),
    booking_source VARCHAR(50),
    base_fare_bdt DECIMAL(15, 2) NOT NULL,
    tax_surcharge_bdt DECIMAL(15, 2) NOT NULL,
    total_fare_bdt DECIMAL(15, 2) NOT NULL,
    seasonality VARCHAR(50),
    days_before_departure INT,
    is_peak_season BOOLEAN DEFAULT FALSE,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Average Fare by Airline
CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    avg_base_fare DECIMAL(15, 2),
    avg_tax_surcharge DECIMAL(15, 2),
    avg_total_fare DECIMAL(15, 2),
    booking_count INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Seasonal Fare Variation
CREATE TABLE IF NOT EXISTS kpi_seasonal_variation (
    id SERIAL PRIMARY KEY,
    seasonality VARCHAR(50) NOT NULL,
    is_peak_season BOOLEAN,
    avg_total_fare DECIMAL(15, 2),
    min_fare DECIMAL(15, 2),
    max_fare DECIMAL(15, 2),
    booking_count INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Popular Routes
CREATE TABLE IF NOT EXISTS kpi_popular_routes (
    id SERIAL PRIMARY KEY,
    source VARCHAR(10) NOT NULL,
    source_name VARCHAR(200),
    destination VARCHAR(10) NOT NULL,
    destination_name VARCHAR(200),
    route_name VARCHAR(300),
    booking_count INT,
    avg_fare DECIMAL(15, 2),
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Booking Count by Airline
CREATE TABLE IF NOT EXISTS kpi_booking_count_by_airline (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    total_bookings INT,
    economy_bookings INT,
    business_bookings INT,
    first_class_bookings INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## Step 3: Create Python Scripts

### 3.1 Data Ingestion Script

Create `scripts/data_ingestion.py`:

```python
"""
Data Ingestion Script
Loads CSV data into MySQL staging table
"""
import pandas as pd
from sqlalchemy import create_engine
import os


def ingest_csv_to_mysql():
    """Load flight price CSV into MySQL staging database."""
    
    # Configuration
    csv_path = '/opt/airflow/data/Flight_Price_Dataset_of_Bangladesh.csv'
    mysql_host = 'mysql'
    mysql_user = 'staging_user'
    mysql_password = 'staging_password'
    mysql_database = 'staging'
    
    print(f"Reading CSV from: {csv_path}")
    
    # Read CSV
    df = pd.read_csv(csv_path)
    print(f"Loaded {len(df)} rows from CSV")
    
    # Rename columns to match database schema
    column_mapping = {
        'Airline': 'airline',
        'Source': 'source',
        'Source Name': 'source_name',
        'Destination': 'destination',
        'Destination Name': 'destination_name',
        'Departure Date & Time': 'departure_datetime',
        'Arrival Date & Time': 'arrival_datetime',
        'Duration (hrs)': 'duration_hrs',
        'Stopovers': 'stopovers',
        'Aircraft Type': 'aircraft_type',
        'Class': 'class',
        'Booking Source': 'booking_source',
        'Base Fare (BDT)': 'base_fare_bdt',
        'Tax & Surcharge (BDT)': 'tax_surcharge_bdt',
        'Total Fare (BDT)': 'total_fare_bdt',
        'Seasonality': 'seasonality',
        'Days Before Departure': 'days_before_departure'
    }
    df = df.rename(columns=column_mapping)
    
    # Convert datetime columns
    df['departure_datetime'] = pd.to_datetime(df['departure_datetime'])
    df['arrival_datetime'] = pd.to_datetime(df['arrival_datetime'])
    
    # Create MySQL connection
    connection_string = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:3306/{mysql_database}"
    engine = create_engine(connection_string)
    
    # Load data into MySQL
    df.to_sql(
        'flight_prices_raw',
        engine,
        if_exists='replace',
        index=False,
        chunksize=5000
    )
    
    print(f"Successfully loaded {len(df)} rows into MySQL staging table")
    return len(df)


if __name__ == "__main__":
    ingest_csv_to_mysql()
```

### 3.2 Data Validation Script

Create `scripts/data_validation.py`:

```python
"""
Data Validation Script
Validates and cleans the staging data
"""
import pandas as pd
from sqlalchemy import create_engine


def validate_data():
    """Validate and clean flight price data."""
    
    # MySQL connection
    mysql_engine = create_engine(
        "mysql+pymysql://staging_user:staging_password@mysql:3306/staging"
    )
    
    # Read raw data
    print("Reading raw data from MySQL...")
    df = pd.read_sql("SELECT * FROM flight_prices_raw", mysql_engine)
    print(f"Loaded {len(df)} rows for validation")
    
    initial_count = len(df)
    
    # ===== VALIDATION CHECKS =====
    
    # 1. Check required columns exist
    required_columns = [
        'airline', 'source', 'destination',
        'base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt'
    ]
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    print("✓ All required columns present")
    
    # 2. Handle missing values
    # Drop rows with null airline, source, or destination
    df = df.dropna(subset=['airline', 'source', 'destination'])
    
    # Fill missing fare values with 0
    df['base_fare_bdt'] = df['base_fare_bdt'].fillna(0)
    df['tax_surcharge_bdt'] = df['tax_surcharge_bdt'].fillna(0)
    
    # Recalculate total fare if missing
    df['total_fare_bdt'] = df.apply(
        lambda row: row['base_fare_bdt'] + row['tax_surcharge_bdt'] 
        if pd.isna(row['total_fare_bdt']) or row['total_fare_bdt'] == 0
        else row['total_fare_bdt'],
        axis=1
    )
    print("✓ Missing values handled")
    
    # 3. Validate data types
    numeric_cols = ['base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt', 'duration_hrs']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    print("✓ Data types validated")
    
    # 4. Consistency checks - remove invalid data
    # Remove negative fares
    df = df[df['base_fare_bdt'] >= 0]
    df = df[df['tax_surcharge_bdt'] >= 0]
    df = df[df['total_fare_bdt'] >= 0]
    
    # Remove rows where total fare doesn't make sense
    df = df[df['total_fare_bdt'] >= df['base_fare_bdt']]
    print("✓ Consistency checks passed")
    
    # 5. Add peak season flag
    peak_seasons = ['Eid', 'Winter Holidays', 'Hajj']
    df['is_peak_season'] = df['seasonality'].isin(peak_seasons)
    print("✓ Peak season flag added")
    
    # Summary
    removed_count = initial_count - len(df)
    print(f"\nValidation Summary:")
    print(f"  - Initial rows: {initial_count}")
    print(f"  - Removed rows: {removed_count}")
    print(f"  - Final rows: {len(df)}")
    
    # Save validated data
    df.to_sql(
        'flight_prices_validated',
        mysql_engine,
        if_exists='replace',
        index=False,
        chunksize=5000
    )
    
    print(f"\n✓ Saved {len(df)} validated rows to MySQL")
    return len(df)


if __name__ == "__main__":
    validate_data()
```

### 3.3 Data Transformation Script

Create `scripts/data_transformation.py`:

```python
"""
Data Transformation Script
Computes KPIs from validated data
"""
import pandas as pd
from sqlalchemy import create_engine


def compute_kpis():
    """Compute KPIs from validated flight data."""
    
    # Database connections
    mysql_engine = create_engine(
        "mysql+pymysql://staging_user:staging_password@mysql:3306/staging"
    )
    pg_engine = create_engine(
        "postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/analytics"
    )
    
    # Read validated data
    print("Reading validated data...")
    df = pd.read_sql("SELECT * FROM flight_prices_validated", mysql_engine)
    print(f"Loaded {len(df)} rows for KPI computation")
    
    # ===== KPI 1: Average Fare by Airline =====
    print("\nComputing KPI: Average Fare by Airline...")
    kpi_airline = df.groupby('airline').agg({
        'base_fare_bdt': 'mean',
        'tax_surcharge_bdt': 'mean',
        'total_fare_bdt': 'mean',
        'airline': 'count'
    }).rename(columns={
        'base_fare_bdt': 'avg_base_fare',
        'tax_surcharge_bdt': 'avg_tax_surcharge',
        'total_fare_bdt': 'avg_total_fare',
        'airline': 'booking_count'
    }).reset_index()
    
    kpi_airline.to_sql('kpi_avg_fare_by_airline', pg_engine, if_exists='replace', index=False)
    print(f"  ✓ Computed for {len(kpi_airline)} airlines")
    
    # ===== KPI 2: Seasonal Fare Variation =====
    print("\nComputing KPI: Seasonal Fare Variation...")
    kpi_seasonal = df.groupby(['seasonality', 'is_peak_season']).agg({
        'total_fare_bdt': ['mean', 'min', 'max', 'count']
    }).reset_index()
    kpi_seasonal.columns = ['seasonality', 'is_peak_season', 'avg_total_fare', 'min_fare', 'max_fare', 'booking_count']
    
    kpi_seasonal.to_sql('kpi_seasonal_variation', pg_engine, if_exists='replace', index=False)
    print(f"  ✓ Computed for {len(kpi_seasonal)} seasons")
    
    # ===== KPI 3: Popular Routes =====
    print("\nComputing KPI: Popular Routes...")
    kpi_routes = df.groupby(['source', 'source_name', 'destination', 'destination_name']).agg({
        'total_fare_bdt': ['count', 'mean']
    }).reset_index()
    kpi_routes.columns = ['source', 'source_name', 'destination', 'destination_name', 'booking_count', 'avg_fare']
    kpi_routes['route_name'] = kpi_routes['source'] + ' → ' + kpi_routes['destination']
    kpi_routes = kpi_routes.nlargest(20, 'booking_count')
    
    kpi_routes.to_sql('kpi_popular_routes', pg_engine, if_exists='replace', index=False)
    print(f"  ✓ Computed top {len(kpi_routes)} routes")
    
    # ===== KPI 4: Booking Count by Airline =====
    print("\nComputing KPI: Booking Count by Airline...")
    kpi_bookings = df.groupby('airline').apply(
        lambda x: pd.Series({
            'total_bookings': len(x),
            'economy_bookings': len(x[x['class'] == 'Economy']),
            'business_bookings': len(x[x['class'] == 'Business']),
            'first_class_bookings': len(x[x['class'] == 'First Class'])
        })
    ).reset_index()
    
    kpi_bookings.to_sql('kpi_booking_count_by_airline', pg_engine, if_exists='replace', index=False)
    print(f"  ✓ Computed for {len(kpi_bookings)} airlines")
    
    print("\n✓ All KPIs computed and saved to PostgreSQL!")
    return True


if __name__ == "__main__":
    compute_kpis()
```

### 3.4 Data Loading Script

Create `scripts/data_loading.py`:

```python
"""
Data Loading Script
Transfers validated data from MySQL to PostgreSQL
"""
import pandas as pd
from sqlalchemy import create_engine


def load_to_postgres():
    """Load validated data from MySQL to PostgreSQL analytics database."""
    
    # Database connections
    mysql_engine = create_engine(
        "mysql+pymysql://staging_user:staging_password@mysql:3306/staging"
    )
    pg_engine = create_engine(
        "postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/analytics"
    )
    
    # Read validated data from MySQL
    print("Reading validated data from MySQL...")
    df = pd.read_sql("SELECT * FROM flight_prices_validated", mysql_engine)
    print(f"Loaded {len(df)} rows")
    
    # Load to PostgreSQL (excluding MySQL auto-increment id)
    columns_to_load = [col for col in df.columns if col not in ['id', 'validated_at']]
    df_to_load = df[columns_to_load]
    
    print("Loading data to PostgreSQL...")
    df_to_load.to_sql(
        'flight_prices',
        pg_engine,
        if_exists='replace',
        index=False,
        chunksize=5000
    )
    
    print(f"✓ Successfully loaded {len(df_to_load)} rows to PostgreSQL analytics database")
    return len(df_to_load)


if __name__ == "__main__":
    load_to_postgres()
```

---

## Step 4: Create the Airflow DAG

### 4.1 Main DAG File

Create `dags/flight_price_dag.py`:

```python
"""
Flight Price Analysis DAG
End-to-end data pipeline for Bangladesh flight price analysis
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

import sys
sys.path.insert(0, '/opt/airflow/scripts')

from data_ingestion import ingest_csv_to_mysql
from data_validation import validate_data
from data_transformation import compute_kpis
from data_loading import load_to_postgres


# Default arguments for the DAG
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='flight_price_analysis',
    default_args=default_args,
    description='ETL pipeline for Bangladesh flight price analysis',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flight', 'etl', 'analytics'],
) as dag:

    # Start task
    start = DummyOperator(task_id='start')

    # Task 1: Ingest CSV to MySQL
    ingest_task = PythonOperator(
        task_id='ingest_csv_to_mysql',
        python_callable=ingest_csv_to_mysql,
    )

    # Task 2: Validate Data
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
    )

    # Task 3: Compute KPIs
    transform_task = PythonOperator(
        task_id='compute_kpis',
        python_callable=compute_kpis,
    )

    # Task 4: Load to PostgreSQL
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
    )

    # End task
    end = DummyOperator(task_id='end')

    # Define task dependencies
    start >> ingest_task >> validate_task >> transform_task >> load_task >> end
```

---

## Step 5: Install Python Dependencies

### 5.1 Create Requirements File

Create `requirements.txt` in your project root:

```
pandas==2.0.3
sqlalchemy==2.0.20
pymysql==1.1.0
psycopg2-binary==2.9.7
```

### 5.2 Install in Airflow Container

```bash
# Access the Airflow container
docker exec -it lab-3-airflow-webserver-1 bash

# Install dependencies
pip install pandas pymysql psycopg2-binary sqlalchemy

# Exit container
exit
```

---

## Step 6: Run and Test the Pipeline

### 6.1 Trigger the DAG

1. Open Airflow UI: http://localhost:8080
2. Find the DAG named `flight_price_analysis`
3. Toggle the DAG ON (click the toggle switch)
4. Click the "Play" button to trigger a manual run

### 6.2 Monitor Progress

Watch the DAG run in the Airflow UI:
- **Green**: Task completed successfully
- **Yellow**: Task running
- **Red**: Task failed (click to see logs)

### 6.3 Verify Results

Connect to PostgreSQL to verify the KPIs:

```bash
# Connect to PostgreSQL analytics database
docker exec -it lab-3-postgres_analytics-1 psql -U analytics_user -d analytics

# Check loaded data
SELECT COUNT(*) FROM flight_prices;

# View average fares by airline
SELECT * FROM kpi_avg_fare_by_airline ORDER BY avg_total_fare DESC LIMIT 10;

# View seasonal variation
SELECT * FROM kpi_seasonal_variation;

# View popular routes
SELECT * FROM kpi_popular_routes ORDER BY booking_count DESC;

# View booking counts
SELECT * FROM kpi_booking_count_by_airline ORDER BY total_bookings DESC LIMIT 10;

# Exit
\q
```

---

## Step 7: Documentation

### 7.1 Create Pipeline Report

Create `docs/pipeline_report.md`:

```markdown
# Flight Price Analysis Pipeline Report

## 1. Pipeline Architecture

[Include the Mermaid diagram from the walkthrough]

## 2. DAG/Task Descriptions

| Task | Description |
|------|-------------|
| ingest_csv_to_mysql | Loads raw CSV data into MySQL staging |
| validate_data | Validates columns, handles nulls, flags inconsistencies |
| compute_kpis | Calculates business metrics |
| load_to_postgres | Transfers cleaned data to PostgreSQL |

## 3. KPI Definitions

### Average Fare by Airline
- **Formula**: `AVG(total_fare_bdt) GROUP BY airline`
- **Purpose**: Compare pricing across carriers

### Seasonal Fare Variation
- **Peak Seasons**: Eid, Winter Holidays, Hajj
- **Formula**: Compare `AVG(total_fare_bdt)` between peak and regular seasons

### Popular Routes
- **Formula**: `COUNT(*) GROUP BY source, destination`
- **Purpose**: Identify high-demand routes

### Booking Count by Airline
- **Formula**: `COUNT(*) GROUP BY airline, class`
- **Purpose**: Market share analysis

## 4. Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| Large dataset | Used chunked loading (5000 rows) |
| Missing values | Imputed with defaults or dropped |
| Data type issues | Used pandas type conversion |
```

---

## 🎯 Checklist

Use this checklist to track your progress:

- [ ] Docker Compose file created
- [ ] MySQL schema created
- [ ] PostgreSQL schema created
- [ ] Data ingestion script created
- [ ] Data validation script created
- [ ] Data transformation script created
- [ ] Data loading script created
- [ ] Airflow DAG created
- [ ] Dependencies installed
- [ ] Pipeline tested successfully
- [ ] Documentation completed

---

## 🆘 Troubleshooting

### Common Issues

1. **Container not starting**: Run `docker-compose logs <service-name>` to check errors
2. **Airflow DAG not showing**: Check `dags/` folder permissions and DAG syntax
3. **Database connection errors**: Verify container names match connection strings
4. **Import errors in DAG**: Ensure scripts are in the mounted volume path

### Useful Commands

```bash
# View all container logs
docker-compose logs -f

# Restart a specific service
docker-compose restart airflow-webserver

# Stop all services
docker-compose down

# Remove all data and start fresh
docker-compose down -v
docker-compose up -d
```

---

**Good luck with your lab! 🚀**
