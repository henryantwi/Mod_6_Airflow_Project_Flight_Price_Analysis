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
