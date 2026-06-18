-- 1. Define the table use incremental model
{{
    config(
        materialized='incremental',
        unique_key=['city', 'observation_time']
    )
}}

SELECT 
    city,
    weather AS weather_condition,
    -- Kelvin to Celsius: C = K - 273.15
    ROUND(temp_k - 273.15, 2) AS temperature_celsius,
    -- Transfer from Unix timestamp to human-readable standard time
    TO_TIMESTAMP(dt) AS observation_time
FROM main.raw_weather_data

{% if is_incremental() %}
    -- 2. Incremental model: Only select the latest observations to avoid duplicates
    WHERE TO_TIMESTAMP(dt) > (SELECT MAX(observation_time) FROM {{ this }})
{% endif %} 

