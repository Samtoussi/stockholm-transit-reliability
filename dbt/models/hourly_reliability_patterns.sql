with base as (

    select
        transport_mode,
        scheduled_arrival_seconds,
        arrival_delay_seconds

    from {{ ref('reliability_stop_observations') }}

    where scheduled_arrival_seconds is not null
      and arrival_delay_seconds is not null

),

with_hour as (

    select
        transport_mode,

        cast(
            pmod(
                floor(scheduled_arrival_seconds / 3600),
                24
            ) as int
        ) as scheduled_hour,

        arrival_delay_seconds

    from base

)

select
    transport_mode,
    scheduled_hour,

    count(*) as observations,

    round(avg(arrival_delay_seconds), 2)
        as avg_arrival_delay_seconds,

    percentile_approx(
        arrival_delay_seconds,
        0.5
    ) as median_arrival_delay_seconds,

    percentile_approx(
        arrival_delay_seconds,
        0.95
    ) as p95_arrival_delay_seconds,

    round(
        100.0 * avg(
            case
                when abs(arrival_delay_seconds) <= 60 then 1
                else 0
            end
        ),
        2
    ) as on_time_pct,

    round(
        100.0 * avg(
            case
                when arrival_delay_seconds > 300 then 1
                else 0
            end
        ),
        2
    ) as over_5_min_late_pct,

    round(
        100.0 * avg(
            case
                when arrival_delay_seconds > 600 then 1
                else 0
            end
        ),
        2
    ) as over_10_min_late_pct

from with_hour

group by
    transport_mode,
    scheduled_hour