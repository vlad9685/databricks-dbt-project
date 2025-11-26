SELECT
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_timestamp,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_timestamp,
    CAST(passenger_count AS INT) AS passenger_count,
    trip_distance,
    pulocationid AS pickup_location_id,
    dolocationid AS dropoff_location_id,
    payment_type,
    fare_amount,
    tip_amount,
    total_amount
FROM {{ source('nyc_transport_data', 'bronze_taxi_trips') }}
WHERE trip_distance > 0 AND fare_amount > 0 -- Basic data quality filter