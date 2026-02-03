import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
import time
import hashlib

# Configure logger
logger = logging.getLogger(__name__)


def generate_validation_hash(df):
    """Generate hash for each row to detect changes."""
    key_string = (
        df['airline'].fillna('').astype(str) + '|' +
        df['source'].fillna('').astype(str) + '|' +
        df['destination'].fillna('').astype(str) + '|' +
        df['departure_datetime'].fillna('').astype(str) + '|' +
        df['class'].fillna('').astype(str) + '|' +
        df['booking_source'].fillna('').astype(str)
    )
    return key_string.apply(lambda x: hashlib.sha256(x.encode()).hexdigest())


def get_validated_hashes(engine):
    """Get hashes of already validated records."""
    try:
        query = "SELECT record_hash FROM flight_prices_validated"
        with engine.connect() as conn:
            result = conn.execute(text(query))
            return set(row[0] for row in result if row[0])
    except Exception as e:
        logger.info(f"Could not fetch validated hashes: {e}")
        return set()


def ensure_validated_table_exists(engine):
    """Create validated table if it doesn't exist."""
    create_sql = """
    CREATE TABLE IF NOT EXISTS flight_prices_validated (
        id INT AUTO_INCREMENT PRIMARY KEY,
        record_hash VARCHAR(64),
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
        validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY unique_record (record_hash)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def batch_insert_validated(df, engine):
    """Batch insert validated records using INSERT IGNORE."""
    if df.empty:
        return 0
    
    columns = [col for col in df.columns if col != 'id']
    escaped_columns = [f'`{col}`' for col in columns]
    column_names = ', '.join(escaped_columns)
    
    df_clean = df.copy()
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    records = df_clean.to_dict('records')
    
    chunk_size = 1000
    inserted_count = 0
    
    with engine.begin() as conn:
        for start in range(0, len(records), chunk_size):
            end = min(start + chunk_size, len(records))
            chunk = records[start:end]
            
            values_list = []
            for record in chunk:
                escaped_values = []
                for col in columns:
                    val = record.get(col)
                    if val is None:
                        escaped_values.append('NULL')
                    elif isinstance(val, bool):
                        escaped_values.append('1' if val else '0')
                    elif isinstance(val, str):
                        escaped_val = val.replace("'", "''")
                        escaped_values.append(f"'{escaped_val}'")
                    elif isinstance(val, (int, float)):
                        escaped_values.append(str(val))
                    else:
                        escaped_values.append(f"'{val}'")
                values_list.append(f"({', '.join(escaped_values)})")
            
            # Use INSERT IGNORE to skip duplicates
            batch_sql = f"""
            INSERT IGNORE INTO flight_prices_validated ({column_names})
            VALUES {', '.join(values_list)}
            """
            conn.execute(text(batch_sql))
            inserted_count += len(chunk)
    
    return inserted_count


def validate_data():
    """Validate and clean flight price data - INCREMENTAL version."""
    
    total_start = time.time()
    
    # STEP 1: MySQL connection
    step_start = time.time()
    mysql_engine = create_engine(
        "mysql+pymysql://staging_user:staging_password@mysql:3306/staging"
    )
    ensure_validated_table_exists(mysql_engine)
    logger.info(f"⏱️ STEP 1 - DB setup: {time.time() - step_start:.2f}s")
    
    # STEP 2: Get already validated hashes
    step_start = time.time()
    validated_hashes = get_validated_hashes(mysql_engine)
    logger.info(f"⏱️ STEP 2 - Fetch validated hashes ({len(validated_hashes)}): {time.time() - step_start:.2f}s")
    
    # STEP 3: Read raw data
    step_start = time.time()
    df = pd.read_sql("SELECT * FROM flight_prices_raw", mysql_engine)
    initial_count = len(df)
    logger.info(f"⏱️ STEP 3 - Read raw data ({len(df)} rows): {time.time() - step_start:.2f}s")
    
    # STEP 4: Generate hashes and filter to new records
    step_start = time.time()
    df['record_hash'] = generate_validation_hash(df)
    
    if validated_hashes:
        df_new = df[~df['record_hash'].isin(validated_hashes)]
        logger.info(f"⏱️ STEP 4 - Filter new ({len(df_new)} new, {len(df) - len(df_new)} skipped): {time.time() - step_start:.2f}s")
    else:
        df_new = df
        logger.info(f"⏱️ STEP 4 - All records are new: {time.time() - step_start:.2f}s")
    
    if df_new.empty:
        logger.info("No new records to validate - done!")
        logger.info(f"✅ VALIDATION TOTAL TIME: {time.time() - total_start:.2f}s")
        return 0
    
    # STEP 5: Validation checks on new records only
    step_start = time.time()
    
    required_columns = ['airline', 'source', 'destination', 'base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt']
    missing_cols = [col for col in required_columns if col not in df_new.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    df_new = df_new.dropna(subset=['airline', 'source', 'destination'])
    df_new['base_fare_bdt'] = df_new['base_fare_bdt'].fillna(0)
    df_new['tax_surcharge_bdt'] = df_new['tax_surcharge_bdt'].fillna(0)
    
    needs_recalc = df_new['total_fare_bdt'].isna() | (df_new['total_fare_bdt'] == 0)
    df_new = df_new.copy()
    df_new.loc[:, 'total_fare_bdt'] = np.where(
        needs_recalc,
        df_new['base_fare_bdt'] + df_new['tax_surcharge_bdt'],
        df_new['total_fare_bdt']
    )
    
    numeric_cols = ['base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt', 'duration_hrs']
    for col in numeric_cols:
        df_new[col] = pd.to_numeric(df_new[col], errors='coerce')
    
    valid_mask = (
        (df_new['base_fare_bdt'] >= 0) &
        (df_new['tax_surcharge_bdt'] >= 0) &
        (df_new['total_fare_bdt'] >= 0) &
        (df_new['total_fare_bdt'] >= df_new['base_fare_bdt'])
    )
    df_new = df_new[valid_mask]
    
    peak_seasons = ['Eid', 'Winter Holidays', 'Hajj']
    df_new = df_new.copy()
    df_new['is_peak_season'] = df_new['seasonality'].isin(peak_seasons)
    
    logger.info(f"⏱️ STEP 5 - Validation checks: {time.time() - step_start:.2f}s")
    
    # STEP 6: Save validated data (incremental insert)
    step_start = time.time()
    rows_inserted = batch_insert_validated(df_new, mysql_engine)
    logger.info(f"⏱️ STEP 6 - Insert validated ({rows_inserted} rows): {time.time() - step_start:.2f}s")
    
    logger.info(f"✅ VALIDATION TOTAL TIME: {time.time() - total_start:.2f}s")
    return len(df_new)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    validate_data()
