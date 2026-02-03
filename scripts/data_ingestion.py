import pandas as pd
from sqlalchemy import create_engine, text
import logging
import glob
import os
import hashlib
import time

# Configure logger
logger = logging.getLogger(__name__)


def build_key_strings(df):
    """Build key strings for all records using VECTORIZED operations."""
    return (
        df['airline'].fillna('').astype(str) + '|' +
        df['source'].fillna('').astype(str) + '|' +
        df['destination'].fillna('').astype(str) + '|' +
        df['departure_datetime'].fillna('').astype(str) + '|' +
        df['class'].fillna('').astype(str) + '|' +
        df['booking_source'].fillna('').astype(str)
    )


def generate_hashes_fast(key_strings):
    """Generate hashes from key strings - optimized."""
    return key_strings.apply(lambda x: hashlib.sha256(x.encode()).hexdigest())


def ensure_table_exists(engine):
    """Create the flight_prices_raw table with UPSERT support if it doesn't exist."""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS flight_prices_raw (
        id INT AUTO_INCREMENT PRIMARY KEY,
        record_hash VARCHAR(64) NOT NULL,
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
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY unique_record (record_hash)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))


def get_existing_hashes(engine):
    """Fetch all existing record hashes from the database."""
    try:
        query = "SELECT record_hash FROM flight_prices_raw"
        with engine.connect() as conn:
            result = conn.execute(text(query))
            hashes = set(row[0] for row in result)
            return hashes
    except Exception as e:
        logger.info(f"Could not fetch existing hashes: {e}")
        return set()


def upsert_to_mysql(df, engine, table_name):
    """Perform batch UPSERT."""
    if df.empty:
        return 0
    
    columns = [col for col in df.columns if col != 'id']
    escaped_columns = [f'`{col}`' for col in columns]
    column_names = ', '.join(escaped_columns)
    
    update_columns = [col for col in columns if col != 'record_hash']
    update_clause = ', '.join([f'`{col}` = VALUES(`{col}`)' for col in update_columns])
    
    df_clean = df.copy()
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    records = df_clean.to_dict('records')
    
    chunk_size = 1000
    total_rows = len(records)
    inserted_count = 0
    
    with engine.begin() as conn:
        for start in range(0, total_rows, chunk_size):
            end = min(start + chunk_size, total_rows)
            chunk = records[start:end]
            
            values_list = []
            for record in chunk:
                escaped_values = []
                for col in columns:
                    val = record.get(col)
                    if val is None:
                        escaped_values.append('NULL')
                    elif isinstance(val, str):
                        escaped_val = val.replace("'", "''")
                        escaped_values.append(f"'{escaped_val}'")
                    elif isinstance(val, (int, float)):
                        escaped_values.append(str(val))
                    else:
                        escaped_values.append(f"'{val}'")
                values_list.append(f"({', '.join(escaped_values)})")
            
            batch_sql = f"""
            INSERT INTO {table_name} ({column_names})
            VALUES {', '.join(values_list)}
            ON DUPLICATE KEY UPDATE {update_clause}
            """
            
            conn.execute(text(batch_sql))
            inserted_count += len(chunk)
    
    return inserted_count


def ingest_csv_to_mysql():
    """Load CSV files with detailed timing logs."""
    
    total_start = time.time()
    
    # Configuration
    data_directory = '/opt/airflow/data'
    mysql_host = 'mysql'
    mysql_user = 'staging_user'
    mysql_password = 'staging_password'
    mysql_database = 'staging'
    
    # STEP 1: Find CSV files
    step_start = time.time()
    csv_pattern = os.path.join(data_directory, '*.csv')
    csv_files = glob.glob(csv_pattern)
    if not csv_files:
        logger.warning(f"No CSV files found in {data_directory}")
        return 0
    logger.info(f"⏱️ STEP 1 - Find files: {time.time() - step_start:.2f}s")
    
    # STEP 2: Read CSV
    step_start = time.time()
    dataframes = []
    for csv_path in csv_files:
        df = pd.read_csv(csv_path)
        dataframes.append(df)
    df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"⏱️ STEP 2 - Read CSV ({len(df)} rows): {time.time() - step_start:.2f}s")
    
    # STEP 3: Rename columns
    step_start = time.time()
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
    logger.info(f"⏱️ STEP 3 - Rename columns: {time.time() - step_start:.2f}s")
    
    # STEP 4: Convert datetimes
    step_start = time.time()
    df['departure_datetime'] = pd.to_datetime(df['departure_datetime'])
    df['arrival_datetime'] = pd.to_datetime(df['arrival_datetime'])
    logger.info(f"⏱️ STEP 4 - Convert datetimes: {time.time() - step_start:.2f}s")
    
    # STEP 5: Build key strings
    step_start = time.time()
    key_strings = build_key_strings(df)
    logger.info(f"⏱️ STEP 5 - Build key strings: {time.time() - step_start:.2f}s")
    
    # STEP 6: Generate hashes
    step_start = time.time()
    df['record_hash'] = generate_hashes_fast(key_strings)
    logger.info(f"⏱️ STEP 6 - Generate hashes: {time.time() - step_start:.2f}s")
    
    # STEP 7: Create DB connection
    step_start = time.time()
    connection_string = f"mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}:3306/{mysql_database}"
    engine = create_engine(connection_string)
    ensure_table_exists(engine)
    logger.info(f"⏱️ STEP 7 - DB connection: {time.time() - step_start:.2f}s")
    
    # STEP 8: Fetch existing hashes
    step_start = time.time()
    existing_hashes = get_existing_hashes(engine)
    logger.info(f"⏱️ STEP 8 - Fetch existing ({len(existing_hashes)}): {time.time() - step_start:.2f}s")
    
    # STEP 9: Filter new records
    step_start = time.time()
    if existing_hashes:
        df_new = df[~df['record_hash'].isin(existing_hashes)]
    else:
        df_new = df
    logger.info(f"⏱️ STEP 9 - Filter new ({len(df_new)} new): {time.time() - step_start:.2f}s")
    
    # STEP 10: UPSERT
    step_start = time.time()
    if df_new.empty:
        logger.info("No new records - done!")
        rows_upserted = 0
    else:
        rows_upserted = upsert_to_mysql(df_new, engine, 'flight_prices_raw')
    logger.info(f"⏱️ STEP 10 - UPSERT ({rows_upserted} rows): {time.time() - step_start:.2f}s")
    
    logger.info(f"✅ TOTAL TIME: {time.time() - total_start:.2f}s")
    return rows_upserted


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_csv_to_mysql()
