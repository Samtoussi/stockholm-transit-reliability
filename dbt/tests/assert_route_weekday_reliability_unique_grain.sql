select
    route_id,
    day_of_week_number,
    count(*) as row_count

from {{ ref('route_weekday_reliability') }}

group by
    route_id,
    day_of_week_number

having count(*) > 1