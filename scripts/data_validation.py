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
