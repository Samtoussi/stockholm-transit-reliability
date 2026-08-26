select
    route_id,
    scheduled_hour,
    count(*) as row_count

from {{ ref('route_hourly_reliability') }}

group by
    route_id,
    scheduled_hour

having count(*) > 1