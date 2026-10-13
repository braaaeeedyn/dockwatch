-- grain: (station_id, hour_of_day 0-23 Pacific). Trip history ends where live data starts, so the two are compared
-- as station x hour-of-day profiles: trips per day over the newest var('trip_months') months, shares over live days.
with s as (select * from {{ ref('stg_station_information') }}),
t as (select * from {{ ref('stg_trips') }}),
months as (select distinct trip_month from t),
days as (
    select
        sum(day_of_month(last_day_of_month(trip_month))) as trip_days,
        min(trip_month) as trip_first_month,
        max(trip_month) as trip_last_month
    from months
),
starts as (
    select s.station_id, t.start_hour as hour_of_day, count(*) as trips
    from t join s on t.start_station_code = s.short_name
    group by 1, 2
),
ends as (
    select s.station_id, t.end_hour as hour_of_day, count(*) as trips
    from t join s on t.end_station_code = s.short_name
    group by 1, 2
),
avail as (
    select
        station_id,
        hour_of_day,
        least(greatest(avg(share_empty), 0.0), 1.0) as share_empty,
        least(greatest(avg(share_full), 0.0), 1.0) as share_full,
        count(distinct service_date) as live_days
    from {{ ref('stg_availability_5m') }}
    group by 1, 2
)
select
    a.station_id,
    s.station_name,
    s.region_id,
    a.hour_of_day,
    coalesce(st.trips, 0) / cast(d.trip_days as double) as trips_started_per_day,
    coalesce(en.trips, 0) / cast(d.trip_days as double) as trips_ended_per_day,
    a.share_empty,
    a.share_full,
    a.live_days,
    d.trip_days,
    d.trip_first_month,
    d.trip_last_month
from avail a
join s on a.station_id = s.station_id
cross join days d
left join starts st on a.station_id = st.station_id and a.hour_of_day = st.hour_of_day
left join ends en on a.station_id = en.station_id and a.hour_of_day = en.hour_of_day
