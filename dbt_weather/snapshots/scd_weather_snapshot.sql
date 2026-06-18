-- snapshots/scd_weather_snapshot.sql

{% snapshot scd_weather_snapshot %}

{{
    config(
      target_schema='main',
      unique_key='city',
      strategy='timestamp',
      updated_at='observation_time',
    )
}}


SELECT 
    city,
    temperature_celsius,
    alert_level,
    observation_time
FROM {{ ref('fct_regional_weather_alerts') }}

{% endsnapshot %}