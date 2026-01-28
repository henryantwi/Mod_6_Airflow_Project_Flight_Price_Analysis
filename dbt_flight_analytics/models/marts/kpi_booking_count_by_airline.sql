{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    KPI: Booking Count by Airline
    
    Total bookings per airline with breakdown by travel class.
*/

SELECT
    airline,
    
    -- Total bookings
    COUNT(*) AS total_bookings,
    
    -- Breakdown by class
    COUNT(*) FILTER (WHERE class = 'Economy') AS economy_bookings,
    COUNT(*) FILTER (WHERE class = 'Business') AS business_bookings,
    COUNT(*) FILTER (WHERE class = 'First Class') AS first_class_bookings

FROM {{ ref('stg_flight_prices') }}
GROUP BY airline
ORDER BY total_bookings DESC
