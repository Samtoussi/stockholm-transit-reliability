select
    service_date,
    trip_id,
    stop_id,
    stop_sequence,
    feed_timestamp,
    count(*) as row_count

from {{ ref('reliability_stop_observations') }}

group by
    service_date,
    trip_id,
    stop_id,
    stop_sequence,
    feed_timestamp

having count(*) > 1