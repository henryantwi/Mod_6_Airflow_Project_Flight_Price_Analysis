import pandas as pd
from sqlalchemy import create_engine
import logging
import glob
import os

# Configure logger
logger = logging.getLogger(__name__)

def ingest_csv_to_mysql():
    """Load all CSV files from data directory into MySQL staging database."""
    
    # ⚠️ TEST ERROR
    # raise Exception("Intentional test error to verify email notifications work!")
    
    # Configuration
    data_directory = '/opt/airflow/data'
    mysql_host = 'mysql'
    mysql_user = 'staging_user'
    mysql_password = 'staging_password'
    mysql_database = 'staging'
    
    # Find all CSV files in the data directory
    csv_pattern = os.path.join(data_directory, '*.csv')
    csv_files = glob.glob(csv_pattern)
    
    if not csv_files:
        logger.warning(f"No CSV files found in {data_directory}")
        return 0
    
    logger.info(f"Found {len(csv_files)} CSV file(s) in {data_directory}")
    
    # Read and combine all CSV files
    dataframes = []
    for csv_path in csv_files:
        logger.info(f"Reading CSV: {os.path.basename(csv_path)}")
        df = pd.read_csv(csv_path)
        logger.info(f"  - Loaded {len(df)} rows")
        dataframes.append(df)
    
    # Combine all dataframes
    df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Total rows from all files: {len(df)}")
    
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
    
    logger.info(f"Successfully loaded {len(df)} rows into MySQL staging table")
    return len(df)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    ingest_csv_to_mysql()
