select station_id, short_name, name as station_name, region_id, lat, lon, capacity
from {{ source('bronze', 'station_information') }}
