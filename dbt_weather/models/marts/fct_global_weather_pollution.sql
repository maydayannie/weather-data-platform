with weather as (
    select * from {{ ref('fct_regional_weather_alerts') }}
),

pollution as (
    select * from {{ ref('stg_pollution') }}
),
joined as (
    select
        w.city,
        w.temperature_celsius,
        w.alert_level as weather_alert,
        p.air_quality_index,
        p.aqi_label,
        p.pm2_5,
        w.observation_time
    from weather w
    inner join pollution p 
        on w.city = p.city 
),
final_insights as (
    select
        *,
        case 
            when weather_alert = 'Comfortable' and air_quality_index <= 2 
                then 'Perfect for Outdoor Sports! '
            when air_quality_index >= 4 
                then 'Warning: Stay Indoors! '
            else 'Acceptable for Casual Walk! '
        end as outdoor_activity_index
    from joined
)

select * from final_insights