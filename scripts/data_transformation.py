import pandas as pd
from sqlalchemy import create_engine
import logging
import time

# Configure logger
logger = logging.getLogger(__name__)


def compute_kpis():
    """Compute KPIs from validated flight data with timing logs."""
    
    total_start = time.time()
    
    # STEP 1: Database connections
    step_start = time.time()
    mysql_engine = create_engine(
        "mysql+pymysql://staging_user:staging_password@mysql:3306/staging"
    )
    pg_engine = create_engine(
        "postgresql+psycopg2://analytics_user:analytics_password@postgres_analytics:5432/analytics"
    )
    logger.info(f"⏱️ STEP 1 - DB connections: {time.time() - step_start:.2f}s")
    
    # STEP 2: Read validated data
    step_start = time.time()
    df = pd.read_sql("SELECT * FROM flight_prices_validated", mysql_engine)
    logger.info(f"⏱️ STEP 2 - Read data ({len(df)} rows): {time.time() - step_start:.2f}s")
    
    # STEP 3: KPI 1 - Average Fare by Airline
    step_start = time.time()
    kpi_airline = df.groupby('airline').agg({
        'base_fare_bdt': 'mean',
        'tax_surcharge_bdt': 'mean',
        'total_fare_bdt': 'mean',
    }).reset_index()
    booking_counts = df.groupby('airline').size().reset_index(name='booking_count')
    kpi_airline = kpi_airline.merge(booking_counts, on='airline')
    kpi_airline = kpi_airline.rename(columns={
        'base_fare_bdt': 'avg_base_fare',
        'tax_surcharge_bdt': 'avg_tax_surcharge',
        'total_fare_bdt': 'avg_total_fare'
    })
    kpi_airline.to_sql('kpi_avg_fare_by_airline', pg_engine, if_exists='replace', index=False)
    logger.info(f"⏱️ STEP 3 - KPI Avg Fare ({len(kpi_airline)} airlines): {time.time() - step_start:.2f}s")
    
    # STEP 4: KPI 2 - Seasonal Variation
    step_start = time.time()
    kpi_seasonal = df.groupby(['seasonality', 'is_peak_season']).agg({
        'total_fare_bdt': ['mean', 'min', 'max', 'count']
    }).reset_index()
    kpi_seasonal.columns = ['seasonality', 'is_peak_season', 'avg_total_fare', 'min_fare', 'max_fare', 'booking_count']
    kpi_seasonal.to_sql('kpi_seasonal_variation', pg_engine, if_exists='replace', index=False)
    logger.info(f"⏱️ STEP 4 - KPI Seasonal ({len(kpi_seasonal)} seasons): {time.time() - step_start:.2f}s")
    
    # STEP 5: KPI 3 - Popular Routes
    step_start = time.time()
    kpi_routes = df.groupby(['source', 'source_name', 'destination', 'destination_name']).agg({
        'total_fare_bdt': ['count', 'mean']
    }).reset_index()
    kpi_routes.columns = ['source', 'source_name', 'destination', 'destination_name', 'booking_count', 'avg_fare']
    kpi_routes['route_name'] = kpi_routes['source'] + ' → ' + kpi_routes['destination']
    kpi_routes = kpi_routes.nlargest(20, 'booking_count')
    kpi_routes.to_sql('kpi_popular_routes', pg_engine, if_exists='replace', index=False)
    logger.info(f"⏱️ STEP 5 - KPI Routes ({len(kpi_routes)} routes): {time.time() - step_start:.2f}s")
    
    # STEP 6: KPI 4 - Booking Count by Airline
    step_start = time.time()
    kpi_bookings = df.groupby('airline').agg(
        total_bookings=('airline', 'size')
    ).reset_index()
    economy = df[df['class'] == 'Economy'].groupby('airline').size().reset_index(name='economy_bookings')
    business = df[df['class'] == 'Business'].groupby('airline').size().reset_index(name='business_bookings')
    first_class = df[df['class'] == 'First Class'].groupby('airline').size().reset_index(name='first_class_bookings')
    kpi_bookings = kpi_bookings.merge(economy, on='airline', how='left')
    kpi_bookings = kpi_bookings.merge(business, on='airline', how='left')
    kpi_bookings = kpi_bookings.merge(first_class, on='airline', how='left')
    kpi_bookings = kpi_bookings.fillna(0)
    kpi_bookings.to_sql('kpi_booking_count_by_airline', pg_engine, if_exists='replace', index=False)
    logger.info(f"⏱️ STEP 6 - KPI Bookings ({len(kpi_bookings)} airlines): {time.time() - step_start:.2f}s")
    
    logger.info(f"✅ TRANSFORMATION TOTAL TIME: {time.time() - total_start:.2f}s")
    return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    compute_kpis()
