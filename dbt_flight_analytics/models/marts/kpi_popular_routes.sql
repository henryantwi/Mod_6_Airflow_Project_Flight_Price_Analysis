{{
    config(
        materialized='table',
        schema='analytics'
    )
}}

/*
    KPI: Popular Routes
    
    Identifies top 20 source-destination pairs by booking count.
    Includes average fare per route.
*/

WITH route_stats AS (
    SELECT
        source,
        source_name,
        destination,
        destination_name,
        
        -- Booking count
        COUNT(*) AS booking_count,
        
        -- Average fare
        ROUND(AVG(total_fare_bdt)::numeric, 2) AS avg_fare

    FROM {{ ref('stg_flight_prices') }}
    GROUP BY source, source_name, destination, destination_name
)

SELECT
    source,
    source_name,
    destination,
    destination_name,
    source || ' → ' || destination AS route_name,
    booking_count,
    avg_fare

FROM route_stats
ORDER BY booking_count DESC
LIMIT 20
