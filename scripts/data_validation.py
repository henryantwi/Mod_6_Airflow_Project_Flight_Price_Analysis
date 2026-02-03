import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import logging

# Configure logger
logger = logging.getLogger(__name__)


def validate_data():
    """Validate and clean flight price data."""
    
    # MySQL connection
    mysql_engine = create_engine(
        "mysql+pymysql://staging_user:staging_password@mysql:3306/staging"
    )
    
    # Read raw data
    logger.info("Reading raw data from MySQL...")
    df = pd.read_sql("SELECT * FROM flight_prices_raw", mysql_engine)
    logger.info(f"Loaded {len(df)} rows for validation")
    
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
    logger.info("All required columns present")
    
    # 2. Handle missing values
    # Drop rows with null airline, source, or destination
    df = df.dropna(subset=['airline', 'source', 'destination'])
    
    # Fill missing fare values with 0
    df['base_fare_bdt'] = df['base_fare_bdt'].fillna(0)
    df['tax_surcharge_bdt'] = df['tax_surcharge_bdt'].fillna(0)
    
    # Recalculate total fare if missing (vectorized - much faster than apply)
    needs_recalc = df['total_fare_bdt'].isna() | (df['total_fare_bdt'] == 0)
    df['total_fare_bdt'] = np.where(
        needs_recalc,
        df['base_fare_bdt'] + df['tax_surcharge_bdt'],
        df['total_fare_bdt']
    )
    logger.info("Missing values handled")
    
    # 3. Validate data types
    numeric_cols = ['base_fare_bdt', 'tax_surcharge_bdt', 'total_fare_bdt', 'duration_hrs']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    logger.info("Data types validated")
    
    # 4. Consistency checks - remove invalid data (combined filter for efficiency)
    valid_mask = (
        (df['base_fare_bdt'] >= 0) &
        (df['tax_surcharge_bdt'] >= 0) &
        (df['total_fare_bdt'] >= 0) &
        (df['total_fare_bdt'] >= df['base_fare_bdt'])
    )
    df = df[valid_mask]
    logger.info("Consistency checks passed")
    
    # 5. Add peak season flag
    peak_seasons = ['Eid', 'Winter Holidays', 'Hajj']
    df['is_peak_season'] = df['seasonality'].isin(peak_seasons)
    logger.info("Peak season flag added")
    
    # Summary
    removed_count = initial_count - len(df)
    logger.info(f"Validation Summary: Initial rows: {initial_count}, Removed rows: {removed_count}, Final rows: {len(df)}")
    
    # Save validated data
    df.to_sql(
        'flight_prices_validated',
        mysql_engine,
        if_exists='replace',
        index=False,
        chunksize=5000
    )
    
    logger.info(f"Saved {len(df)} validated rows to MySQL")
    return len(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    validate_data()
