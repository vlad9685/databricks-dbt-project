-- This is our final Gold model.
-- It aggregates the trip data by day and joins it with the daily weather data.

WITH citibike_daily AS (
    SELECT
        CAST(trip_start_timestamp AS DATE) AS trip_date,
        COUNT(ride_id) AS total_citibike_trips
    FROM {{ ref('silver_citibike_trips') }}
    GROUP BY 1
),

taxi_daily AS (
    -- Filter for valid 2024 data to ensure data quality
    SELECT
        CAST(pickup_timestamp AS DATE) AS trip_date,
        COUNT(*) AS total_taxi_trips,
        SUM(passenger_count) AS total_passengers,
        SUM(total_amount) AS total_revenue
    FROM {{ ref('silver_taxi_trips') }}
    WHERE YEAR(pickup_timestamp) = 2024
    GROUP BY 1
)

-- Final SELECT to join our daily aggregates with the weather data
SELECT
    weather.weather_date,
    weather.temp_max_c,
    weather.temp_min_c,
    weather.precipitation_mm,
    weather.snowfall_cm,
    COALESCE(citibike_daily.total_citibike_trips, 0) AS total_citibike_trips,
    COALESCE(taxi_daily.total_taxi_trips, 0) AS total_taxi_trips,
    COALESCE(taxi_daily.total_passengers, 0) AS total_passengers,
    COALESCE(taxi_daily.total_revenue, 0) AS total_revenue
FROM {{ ref('silver_weather') }} AS weather
LEFT JOIN citibike_daily ON weather.weather_date = citibike_daily.trip_date
LEFT JOIN taxi_daily ON weather.weather_date = taxi_daily.trip_date
ORDER BY weather.weather_date
