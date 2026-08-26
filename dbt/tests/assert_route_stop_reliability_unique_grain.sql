select
    route_id,
    stop_id,
    count(*) as row_count

from {{ ref('route_stop_reliability') }}

group by
    route_id,
    stop_id

having count(*) > 1