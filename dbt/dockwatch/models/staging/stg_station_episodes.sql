-- open episodes end at the newest live window (Spark runs on demand, so "now" would overstate them)
select
    e.station_id,
    e.kind,
    e.alerted,
    e.start_ts,
    coalesce(e.end_ts, l.last_window) as end_ts
from {{ source('silver', 'station_episodes') }} e
cross join (select max(window_end) as last_window from {{ source('silver', 'availability_5m') }}) l
where e.kind in ('empty', 'full')
