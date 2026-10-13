-- the newest var('trip_months') loaded months, with Pacific start / end hours
with months as (
    select trip_month
    from (select distinct trip_month from {{ source('bronze', 'trips') }})
    order by trip_month desc
    limit {{ var('trip_months') }}
)
select
    t.trip_month,
    t.start_station_code,
    t.end_station_code,
    hour(t.started_at at time zone 'America/Los_Angeles') as start_hour,
    hour(t.ended_at at time zone 'America/Los_Angeles') as end_hour
from {{ source('bronze', 'trips') }} t
join months m on t.trip_month = m.trip_month
