SELECT
    ride_id,
    rideable_type,
    CAST(started_at AS TIMESTAMP) AS trip_start_timestamp,
    CAST(ended_at AS TIMESTAMP) AS trip_end_timestamp,
    start_station_name,
    end_station_name,
    member_casual AS member_type
FROM {{ source('nyc_transport_data', 'bronze_citibike_trips') }}