"""Script to sync validated data from MySQL to PostgreSQL for dbt processing."""

import pandas as pd
from sqlalchemy import create_engine


def sync_to_postgres():
    """Sync raw flight data from MySQL to PostgreSQL for dbt."""
    
    # Database connections
    mysql_engine = create_engine(
        "mysql+pymysql://staging_user:staging_password@mysql:3306/staging"
    )
    pg_engine = create_engine(
        "postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/analytics"
    )
    
    # Read raw data from MySQL
    print("Reading raw data from MySQL...")
    df = pd.read_sql("SELECT * FROM flight_prices_raw", mysql_engine)
    print(f"Loaded {len(df)} rows")
    
    # Write to PostgreSQL for dbt to process
    print("Syncing to PostgreSQL...")
    df.to_sql(
        'flight_prices_raw',
        pg_engine,
        if_exists='replace',
        index=False,
        chunksize=5000
    )
    
    print(f"✓ Synced {len(df)} rows to PostgreSQL")
    return len(df)


if __name__ == "__main__":
    sync_to_postgres()
