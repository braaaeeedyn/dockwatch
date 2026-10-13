-- shares are fractions of time: within [0, 1] and never NULL
select 'daily' as mart, station_id
from {{ ref('mart_station_availability_daily') }}
where not (share_empty between 0 and 1 and share_full between 0 and 1)
union all
select 'hourly' as mart, station_id
from {{ ref('mart_trips_vs_availability') }}
where not (share_empty between 0 and 1 and share_full between 0 and 1)
