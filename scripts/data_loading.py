import pandas as pd
from sqlalchemy import create_engine
import logging

# Configure logger
logger = logging.getLogger(__name__)


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
    logger.info("Reading validated data from MySQL...")
    df = pd.read_sql("SELECT * FROM flight_prices_validated", mysql_engine)
    logger.info(f"Loaded {len(df)} rows")
    
    # Load to PostgreSQL (excluding MySQL auto-increment id and validated_at)
    columns_to_exclude = ['id', 'validated_at']
    columns_to_load = [col for col in df.columns if col not in columns_to_exclude]
    df_to_load = df[columns_to_load]
    
    logger.info("Loading data to PostgreSQL...")
    df_to_load.to_sql(
        'flight_prices',
        pg_engine,
        if_exists='replace',
        index=False,
        chunksize=5000
    )
    
    logger.info(f"Successfully loaded {len(df_to_load)} rows to PostgreSQL analytics database")
    return len(df_to_load)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    load_to_postgres()
