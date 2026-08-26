select
    route_id,
    direction_id,
    count(*) as row_count

from {{ ref('route_direction_reliability') }}

group by
    route_id,
    direction_id

having count(*) > 1