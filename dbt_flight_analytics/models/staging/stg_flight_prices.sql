{{
    config(
        materialized='view',
        schema='staging'
    )
}}

/*
    Staging model for flight prices - replaces data_validation.py
    
    Validations performed:
    1. Filter out null airlines, sources, destinations
    2. Handle missing fare values (default to 0)
    3. Recalculate total fare if missing/zero
    4. Validate numeric data types
    5. Remove negative fares
    6. Add is_peak_season flag
*/

WITH source_data AS (
    SELECT *
    FROM {{ source('raw', 'flight_prices_raw') }}
),

-- Step 1: Filter out null required fields
filtered AS (
    SELECT *
    FROM source_data
    WHERE airline IS NOT NULL
      AND source IS NOT NULL
      AND destination IS NOT NULL
),

-- Step 2: Handle missing fare values and recalculate
cleaned AS (
    SELECT
        airline,
        source,
        source_name,
        destination,
        destination_name,
        class,
        duration_hrs,
        seasonality,
        
        -- Handle missing base fare
        COALESCE(base_fare_bdt, 0) AS base_fare_bdt,
        
        -- Handle missing tax/surcharge
        COALESCE(tax_surcharge_bdt, 0) AS tax_surcharge_bdt,
        
        -- Recalculate total fare if missing or zero
        CASE 
            WHEN total_fare_bdt IS NULL OR total_fare_bdt = 0 
            THEN COALESCE(base_fare_bdt, 0) + COALESCE(tax_surcharge_bdt, 0)
            ELSE total_fare_bdt
        END AS total_fare_bdt
    FROM filtered
),

-- Step 3: Validate - remove negative fares and inconsistent data
validated AS (
    SELECT *
    FROM cleaned
    WHERE base_fare_bdt >= 0
      AND tax_surcharge_bdt >= 0
      AND total_fare_bdt >= 0
      AND total_fare_bdt >= base_fare_bdt
),

-- Step 4: Add peak season flag
final AS (
    SELECT
        *,
        CASE 
            WHEN seasonality IN ('Eid', 'Winter Holidays', 'Hajj') THEN TRUE
            ELSE FALSE
        END AS is_peak_season
    FROM validated
)

SELECT * FROM final
