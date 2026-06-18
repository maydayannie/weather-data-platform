WITH current_weather AS(
    SELECT * FROM {{ref('stg_weather')}}
)

SELECT
    city,
    temperature_celsius,
    weather_condition,
    observation_time,
    CASE 
        WHEN temperature_celsius >= 35.0 THEN '🔴 Extreme Heat'
        WHEN temperature_celsius >= 28.0 AND temperature_celsius < 35.0 THEN '🟠 Hot'
        WHEN temperature_celsius >= 18.0 AND temperature_celsius < 28.0 THEN '🟢 Comfortable'
        ELSE '🔵 Cool/Cold'
    END AS alert_level,

    CASE 
        WHEN city IN ('Taipei', 'Xizhi', 'Banqiao') THEN 'Taiwan'
        ELSE 'International'
    END AS region_type
FROM current_weather