-- grain: (station_id, service_date Pacific); one row per current station and day with live data
with a as (select * from {{ ref('stg_availability_5m') }}),
s as (select * from {{ ref('stg_station_information') }})
select
    a.station_id,
    s.station_name,
    s.region_id,
    a.service_date,
    count(*) as windows,
    count(*) / 12.0 as coverage_hours,
    least(greatest(avg(a.share_empty), 0.0), 1.0) as share_empty,
    least(greatest(avg(a.share_full), 0.0), 1.0) as share_full,
    sum(a.share_empty) * 5 as minutes_empty,
    sum(a.share_full) * 5 as minutes_full,
    avg(a.avg_bikes) as avg_bikes,
    min(a.window_start) as first_window,
    max(a.window_start) as last_window
from a
join s on a.station_id = s.station_id
group by a.station_id, s.station_name, s.region_id, a.service_date
