WITH dates AS (
    SELECT
        posexplode(daily.time) AS (pos, weather_date)
    FROM {{ source('nyc_transport_data', 'bronze_weather') }}
),

max_temps AS (
    SELECT
        posexplode(daily.temperature_2m_max) AS (pos, temp_max_c)
    FROM {{ source('nyc_transport_data', 'bronze_weather') }}
),

min_temps AS (
    SELECT
        posexplode(daily.temperature_2m_min) AS (pos, temp_min_c)
    FROM {{ source('nyc_transport_data', 'bronze_weather') }}   
),

precipiation AS (
    SELECT
        posexplode(daily.precipitation_sum) AS (pos, precipitation_mm)
    FROM {{ source('nyc_transport_data', 'bronze_weather') }}       
),

snowfall AS (
    SELECT
        posexplode(daily.snowfall_sum) AS (pos, snowfall_cm)
    FROM {{ source('nyc_transport_data', 'bronze_weather') }}       
)

SELECT
    CAST(d.weather_date AS DATE) AS weather_date,
    t_max.temp_max_c,
    t_min.temp_min_c,
    p.precipitation_mm,
    s.snowfall_cm
FROM dates d
LEFT JOIN max_temps t_max ON d.pos = t_max.pos
LEFT JOIN min_temps t_min ON d.pos = t_min.pos
LEFT JOIN precipiation p ON d.pos = p.pos
LEFT JOIN snowfall s ON d.pos = s.pos
ORDER BY d.weather_date;
