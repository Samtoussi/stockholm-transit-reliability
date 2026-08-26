{{
    config(
        materialized='table'
    )
}}

with observations as (

    select
        *,

        abs(
            unix_timestamp(feed_timestamp)
            - unix_timestamp(arrival_time_actual)
        ) as seconds_from_reported_arrival

    from {{ ref('reliability_stop_observations') }}

    where arrival_time_actual is not null

),

ranked as (

    select
        *,

        row_number() over (
            partition by
                service_date,
                trip_id,
                stop_id,
                stop_sequence

            order by
                seconds_from_reported_arrival asc,
                feed_timestamp desc
        ) as snapshot_rank

    from observations

),

canonical_events as (

    select
        service_date,

        trip_id,
        route_id,
        route_short_name,
        route_long_name,
        transport_mode,

        trip_headsign,
        direction_id,

        stop_id,
        stop_name,
        stop_sequence,

        scheduled_arrival_time,
        scheduled_departure_time,
        scheduled_arrival_seconds,
        scheduled_departure_seconds,

        arrival_time_actual,
        departure_time_actual,

        arrival_delay_seconds,
        departure_delay_seconds,

        feed_timestamp,

        seconds_from_reported_arrival

    from ranked

    where snapshot_rank = 1

)

select *
from canonical_events