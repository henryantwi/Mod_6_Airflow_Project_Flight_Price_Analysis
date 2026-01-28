{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    KPI: Seasonal Fare Variation
    
    Analyzes fare patterns across different seasons,
    comparing peak vs non-peak periods.
*/

SELECT
    seasonality,
    is_peak_season,
    
    -- Fare statistics
    ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare,
    ROUND(MIN(total_fare_bdt)::numeric, 2) AS min_fare,
    ROUND(MAX(total_fare_bdt)::numeric, 2) AS max_fare,
    
    -- Booking count
    COUNT(*) AS booking_count

FROM {{ ref('stg_flight_prices') }}
GROUP BY seasonality, is_peak_season
ORDER BY is_peak_season DESC, booking_count DESC
