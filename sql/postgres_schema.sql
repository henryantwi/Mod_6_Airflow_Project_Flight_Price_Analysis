-- PostgreSQL Analytics Database Schema

-- Processed flight data
CREATE TABLE IF NOT EXISTS flight_prices (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    source VARCHAR(10) NOT NULL,
    source_name VARCHAR(200),
    destination VARCHAR(10) NOT NULL,
    destination_name VARCHAR(200),
    departure_datetime TIMESTAMP,
    arrival_datetime TIMESTAMP,
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
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Average Fare by Airline
CREATE TABLE IF NOT EXISTS kpi_avg_fare_by_airline (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    avg_base_fare DECIMAL(15, 2),
    avg_tax_surcharge DECIMAL(15, 2),
    avg_total_fare DECIMAL(15, 2),
    booking_count INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Seasonal Fare Variation
CREATE TABLE IF NOT EXISTS kpi_seasonal_variation (
    id SERIAL PRIMARY KEY,
    seasonality VARCHAR(50) NOT NULL,
    is_peak_season BOOLEAN,
    avg_total_fare DECIMAL(15, 2),
    min_fare DECIMAL(15, 2),
    max_fare DECIMAL(15, 2),
    booking_count INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Popular Routes
CREATE TABLE IF NOT EXISTS kpi_popular_routes (
    id SERIAL PRIMARY KEY,
    source VARCHAR(10) NOT NULL,
    source_name VARCHAR(200),
    destination VARCHAR(10) NOT NULL,
    destination_name VARCHAR(200),
    route_name VARCHAR(300),
    booking_count INT,
    avg_fare DECIMAL(15, 2),
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- KPI: Booking Count by Airline
CREATE TABLE IF NOT EXISTS kpi_booking_count_by_airline (
    id SERIAL PRIMARY KEY,
    airline VARCHAR(100) NOT NULL,
    total_bookings INT,
    economy_bookings INT,
    business_bookings INT,
    first_class_bookings INT,
    computed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
