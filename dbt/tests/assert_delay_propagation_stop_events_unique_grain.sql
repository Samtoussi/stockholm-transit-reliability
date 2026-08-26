select
    service_date,
    trip_id,
    stop_id,
    stop_sequence,
    count(*) as row_count

from {{ ref('delay_propagation_stop_events') }}

group by
    service_date,
    trip_id,
    stop_id,
    stop_sequence

having count(*) > 1