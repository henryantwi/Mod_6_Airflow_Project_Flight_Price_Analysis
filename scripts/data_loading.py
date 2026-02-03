import pandas as pd
from sqlalchemy import create_engine, text
import logging
import time
import hashlib

# Configure logger
logger = logging.getLogger(__name__)


def generate_record_hash(df):
    """Generate hash for each row."""
    key_string = (
        df['airline'].fillna('').astype(str) + '|' +
        df['source'].fillna('').astype(str) + '|' +
        df['destination'].fillna('').astype(str) + '|' +
        df['departure_datetime'].fillna('').astype(str) + '|' +
        df['class'].fillna('').astype(str) + '|' +
        df['booking_source'].fillna('').astype(str)
    )
    return key_string.apply(lambda x: hashlib.sha256(x.encode()).hexdigest())


def get_loaded_hashes(engine):
    """Get hashes of already loaded records in PostgreSQL."""
    try:
        query = "SELECT record_hash FROM flight_prices WHERE record_hash IS NOT NULL"
        with engine.connect() as conn:
            result = conn.execute(text(query))
            return set(row[0] for row in result if row[0])
    except Exception as e:
        logger.info(f"Could not fetch loaded hashes: {e}")
        return set()



def ensure_postgres_table_exists(engine):
    """Create flight_prices table if it doesn't exist, and ensure unique constraint."""
    # First, try to create table
    create_sql = """
    CREATE TABLE IF NOT EXISTS flight_prices (
        id SERIAL PRIMARY KEY,
        record_hash VARCHAR(64),
        airline VARCHAR(100),
        source VARCHAR(10),
        source_name VARCHAR(200),
        destination VARCHAR(10),
        destination_name VARCHAR(200),
        departure_datetime TIMESTAMP,
        arrival_datetime TIMESTAMP,
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
        is_peak_season BOOLEAN DEFAULT FALSE,
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Add record_hash column if it doesn't exist (migration for existing tables)
    add_column_sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name = 'flight_prices' AND column_name = 'record_hash'
        ) THEN
            ALTER TABLE flight_prices ADD COLUMN record_hash VARCHAR(64);
        END IF;
    END $$;
    """
    
    # Add unique constraint if it doesn't exist
    add_constraint_sql = """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_constraint 
            WHERE conname = 'flight_prices_record_hash_key'
        ) THEN
            ALTER TABLE flight_prices ADD CONSTRAINT flight_prices_record_hash_key UNIQUE (record_hash);
        END IF;
    END $$;
    """
    
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(text(add_column_sql))  # Add column first
        conn.execute(text(add_constraint_sql))



def batch_insert_postgres(df, engine):
    """Batch insert records using INSERT ON CONFLICT DO NOTHING."""
    if df.empty:
        return 0
    
    columns = [col for col in df.columns if col not in ['id', 'validated_at']]
    column_names = ', '.join(columns)
    
    df_clean = df[columns].copy()
    df_clean = df_clean.where(pd.notnull(df_clean), None)
    
    # Convert is_peak_season from MySQL TINYINT (0/1) to Python bool
    if 'is_peak_season' in df_clean.columns:
        df_clean['is_peak_season'] = df_clean['is_peak_season'].apply(
            lambda x: bool(x) if x is not None else None
        )
    
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
                        escaped_values.append('TRUE' if val else 'FALSE')
                    elif isinstance(val, str):
                        escaped_val = val.replace("'", "''")
                        escaped_values.append(f"'{escaped_val}'")
                    elif isinstance(val, (int, float)):
                        escaped_values.append(str(val))
                    else:
                        escaped_values.append(f"'{val}'")
                values_list.append(f"({', '.join(escaped_values)})")
            
            # Use ON CONFLICT DO NOTHING to skip duplicates
            batch_sql = f"""
            INSERT INTO flight_prices ({column_names})
            VALUES {', '.join(values_list)}
            ON CONFLICT (record_hash) DO NOTHING
            """
            conn.execute(text(batch_sql))
            inserted_count += len(chunk)
    
    return inserted_count


def load_to_postgres():
    """Load validated data from MySQL to PostgreSQL - INCREMENTAL version."""
    
    total_start = time.time()
    
    # STEP 1: Database connections
    step_start = time.time()
    mysql_engine = create_engine(
        "mysql+pymysql://staging_user:staging_password@mysql:3306/staging"
    )
    pg_engine = create_engine(
        "postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/analytics"
    )
    ensure_postgres_table_exists(pg_engine)
    logger.info(f"⏱️ STEP 1 - DB setup: {time.time() - step_start:.2f}s")
    
    # STEP 2: Get already loaded hashes
    step_start = time.time()
    loaded_hashes = get_loaded_hashes(pg_engine)
    logger.info(f"⏱️ STEP 2 - Fetch loaded hashes ({len(loaded_hashes)}): {time.time() - step_start:.2f}s")
    
    # STEP 3: Read validated data from MySQL
    step_start = time.time()
    df = pd.read_sql("SELECT * FROM flight_prices_validated", mysql_engine)
    logger.info(f"⏱️ STEP 3 - Read from MySQL ({len(df)} rows): {time.time() - step_start:.2f}s")
    
    # STEP 4: Generate hashes and filter to new records
    step_start = time.time()
    
    # Check if record_hash already exists in the data
    if 'record_hash' not in df.columns:
        df['record_hash'] = generate_record_hash(df)
    
    if loaded_hashes:
        df_new = df[~df['record_hash'].isin(loaded_hashes)]
        logger.info(f"⏱️ STEP 4 - Filter new ({len(df_new)} new, {len(df) - len(df_new)} skipped): {time.time() - step_start:.2f}s")
    else:
        df_new = df
        logger.info(f"⏱️ STEP 4 - All records are new: {time.time() - step_start:.2f}s")
    
    if df_new.empty:
        logger.info("No new records to load - done!")
        logger.info(f"✅ LOADING TOTAL TIME: {time.time() - total_start:.2f}s")
        return 0
    
    # STEP 5: Load to PostgreSQL (incremental insert)
    step_start = time.time()
    rows_inserted = batch_insert_postgres(df_new, pg_engine)
    logger.info(f"⏱️ STEP 5 - Insert to PostgreSQL ({rows_inserted} rows): {time.time() - step_start:.2f}s")
    
    logger.info(f"✅ LOADING TOTAL TIME: {time.time() - total_start:.2f}s")
    return len(df_new)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_to_postgres()
