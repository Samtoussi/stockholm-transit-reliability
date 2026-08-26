select
    route_id,
    count(*) as row_count

from {{ ref('route_delay_propagation') }}

group by route_id

having count(*) > 1