-- 5-minute windows with their Pacific service date and hour of day
select
    station_id,
    window_start,
    cast(window_start at time zone 'America/Los_Angeles' as date) as service_date,
    hour(window_start at time zone 'America/Los_Angeles') as hour_of_day,
    avg_bikes,
    share_empty,
    share_full,
    share_offline
from {{ source('silver', 'availability_5m') }}
where share_empty is not null and share_full is not null
