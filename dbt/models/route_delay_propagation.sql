with propagation_events as (

    select
        route_id,
        route_short_name,
        route_long_name,
        transport_mode,

        delay_change_seconds,
        is_extreme_propagation

    from {{ ref('delay_propagation_stop_events') }}

    where delay_change_seconds is not null

),

aggregated as (

    select
        route_id,
        route_short_name,
        route_long_name,
        transport_mode,

        count(*) as propagation_events,

        sum(
            case
                when is_extreme_propagation then 1
                else 0
            end
        ) as extreme_propagation_events,

        round(
            100.0 * avg(
                case
                    when is_extreme_propagation then 1
                    else 0
                end
            ),
            2
        ) as extreme_propagation_pct,

        round(
            avg(
                case
                    when not is_extreme_propagation
                    then delay_change_seconds
                end
            ),
            2
        ) as avg_delay_change_seconds,

        percentile_approx(
            case
                when not is_extreme_propagation
                then delay_change_seconds
            end,
            0.5
        ) as median_delay_change_seconds,

        round(
            100.0 * avg(
                case
                    when not is_extreme_propagation
                        and delay_change_seconds > 0
                        then 1

                    when not is_extreme_propagation
                        then 0

                    else null
                end
            ),
            2
        ) as pct_delay_accumulated,

        round(
            100.0 * avg(
                case
                    when not is_extreme_propagation
                        and delay_change_seconds < 0
                        then 1

                    when not is_extreme_propagation
                        then 0

                    else null
                end
            ),
            2
        ) as pct_delay_recovered

    from propagation_events

    group by
        route_id,
        route_short_name,
        route_long_name,
        transport_mode

)

select *
from aggregated