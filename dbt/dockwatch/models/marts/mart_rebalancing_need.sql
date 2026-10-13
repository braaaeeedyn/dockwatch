-- grain: station_id; hours empty / full over the newest var('need_days') Pacific days of live data, ranked
with live as (
    select max(service_date) as last_day, max(service_date) - interval '{{ var("need_days") }}' day as before_first
    from {{ ref('stg_availability_5m') }}
),
a as (
    select a.*
    from {{ ref('stg_availability_5m') }} a
    cross join live
    where a.service_date > live.before_first
),
hours as (
    select
        station_id,
        sum(share_empty) * 5 / 60.0 as hours_empty,
        sum(share_full) * 5 / 60.0 as hours_full,
        count(*) / 12.0 as coverage_hours
    from a
    group by station_id
),
ep as (
    select
        e.station_id,
        count(*) as episodes,
        count_if(e.alerted) as alerted_episodes,
        max(case when e.kind = 'empty' then date_diff('second', e.start_ts, e.end_ts) end) / 3600.0
            as longest_empty_hours
    from {{ ref('stg_station_episodes') }} e
    cross join live
    where cast(e.start_ts at time zone 'America/Los_Angeles' as date) > live.before_first
    group by e.station_id
)
select
    h.station_id,
    s.station_name,
    s.region_id,
    h.hours_empty,
    h.hours_full,
    rank() over (order by h.hours_empty + h.hours_full desc) as need_rank,
    coalesce(ep.episodes, 0) as episodes,
    coalesce(ep.alerted_episodes, 0) as alerted_episodes,
    coalesce(ep.longest_empty_hours, 0) as longest_empty_hours,
    h.coverage_hours,
    live.before_first + interval '1' day as window_first_day,
    live.last_day as window_last_day
from hours h
join {{ ref('stg_station_information') }} s on h.station_id = s.station_id
cross join live
left join ep on h.station_id = ep.station_id
