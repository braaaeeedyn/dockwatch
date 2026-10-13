-- one row per (station_id, hour_of_day), hours 0-23 only
select station_id, hour_of_day, count(*) as n
from {{ ref('mart_trips_vs_availability') }}
group by 1, 2
having count(*) > 1 or hour_of_day not between 0 and 23
