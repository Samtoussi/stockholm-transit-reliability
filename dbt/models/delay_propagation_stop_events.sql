with base as (

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

        scheduled_arrival_seconds,

        arrival_delay_seconds

    from {{ ref('reliability_stop_events') }}

    where arrival_delay_seconds is not null

),

with_previous_stop as (

    select
        *,

        lag(stop_id) over (
            partition by
                service_date,
                trip_id
            order by stop_sequence
        ) as previous_stop_id,

        lag(stop_name) over (
            partition by
                service_date,
                trip_id
            order by stop_sequence
        ) as previous_stop_name,

        lag(arrival_delay_seconds) over (
            partition by
                service_date,
                trip_id
            order by stop_sequence
        ) as previous_arrival_delay_seconds

    from base

),

propagation as (

    select
        *,

        arrival_delay_seconds
            - previous_arrival_delay_seconds
            as delay_change_seconds,

        case
            when previous_arrival_delay_seconds is null
                then null

            when abs(
                arrival_delay_seconds
                - previous_arrival_delay_seconds
            ) > 600
                then true

            else false
        end as is_extreme_propagation

    from with_previous_stop

)

select *
from propagation