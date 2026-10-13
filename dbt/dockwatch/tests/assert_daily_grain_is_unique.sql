-- one row per (station_id, service_date)
select station_id, service_date, count(*) as n
from {{ ref('mart_station_availability_daily') }}
group by 1, 2
having count(*) > 1
