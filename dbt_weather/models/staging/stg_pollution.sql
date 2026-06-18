-- models/staging/stg_pollution.sql

with source as (
    select * from {{ source('src_weather', 'raw_pollution_data') }}
),

renamed as (
    select
        city,
        aqi as air_quality_index,
        pm2_5,
        pm10,
        to_timestamp(dt) as observation_time,
        
        case aqi
            when 1 then '🟢 Good'
            when 2 then '🟡 Fair'
            when 3 then '🟠 Moderate'
            when 4 then '🔴 Poor'
            when 5 then '🟣 Very Poor'
            else '⚪ Unknown'
        end as aqi_label
    from source
)

select * from renamed