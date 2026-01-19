-- MySQL Staging Database Schema
USE staging;

-- Raw flight prices table (matches CSV structure)
CREATE TABLE IF NOT EXISTS flight_prices_raw (
    id INT AUTO_INCREMENT PRIMARY KEY,
    airline VARCHAR(100),
    source VARCHAR(10),
    source_name VARCHAR(200),
    destination VARCHAR(10),
    destination_name VARCHAR(200),
    departure_datetime DATETIME,
    arrival_datetime DATETIME,
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Validated flight prices table
CREATE TABLE IF NOT EXISTS flight_prices_validated (
    id INT AUTO_INCREMENT PRIMARY KEY,
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
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
