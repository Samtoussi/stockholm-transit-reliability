with realtime as (

    select
        cast(trip_id as bigint) as trip_id,
        cast(stop_id as bigint) as stop_id,
        cast(stop_sequence as int) as stop_sequence,
        arrival_time_actual,
        departure_time_actual,
        arrival_delay_seconds,
        departure_delay_seconds,
        feed_timestamp,
        start_date

    from {{ source('silver', 'realtime_stop_events') }}

),

scheduled as (

    select
        trip_id,
        stop_id,
        stop_sequence,
        arrival_time_raw,
        departure_time_raw,
        arrival_seconds,
        departure_seconds

    from {{ source('silver', 'stop_events') }}

),

trips as (

    select
        trip_id,
        route_id,
        trip_headsign,
        direction_id

    from {{ source('silver', 'trips') }}

),

routes as (

    select
        route_id,
        route_short_name,
        route_long_name,
        transport_mode

    from {{ source('silver', 'routes') }}

),

stops as (

    select
        stop_id,
        stop_name

    from {{ source('silver', 'stops') }}

)

select
    r.start_date as service_date,

    r.trip_id,
    t.route_id,

    rt.route_short_name,
    rt.route_long_name,
    rt.transport_mode,

    t.trip_headsign,
    t.direction_id,

    r.stop_id,
    st.stop_name,
    r.stop_sequence,

    r.feed_timestamp,

    s.arrival_time_raw as scheduled_arrival_time,
    s.departure_time_raw as scheduled_departure_time,
    s.arrival_seconds as scheduled_arrival_seconds,
    s.departure_seconds as scheduled_departure_seconds,

    r.arrival_time_actual,
    r.departure_time_actual,

    r.arrival_delay_seconds,
    r.departure_delay_seconds

from realtime r

inner join scheduled s
    on r.trip_id = s.trip_id
    and r.stop_id = s.stop_id
    and r.stop_sequence = s.stop_sequence

inner join trips t
    on r.trip_id = t.trip_id

inner join routes rt
    on t.route_id = rt.route_id

inner join stops st
    on r.stop_id = st.stop_id