{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    KPI: Average Fare by Airline
    
    Computes average base fare, tax/surcharge, and total fare per airline.
    Also includes booking count for context.
*/

SELECT
    airline,
    
    -- Average fares
    ROUND(AVG(base_fare_bdt)::numeric, 2) AS avg_base_fare,
    ROUND(AVG(tax_surcharge_bdt)::numeric, 2) AS avg_tax_surcharge,
    ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_total_fare,
    
    -- Booking count
    COUNT(*) AS booking_count

FROM {{ ref('stg_flight_prices') }}
GROUP BY airline
ORDER BY booking_count DESC
