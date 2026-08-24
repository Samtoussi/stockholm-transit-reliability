from pyspark.sql import DataFrame
from pyspark.sql import functions as F


STOCKHOLM_AGENCY_IDS = [
    505000000000000001,
    500000000000000114,
]

TRANSPORT_MODE_MAP = {
    106: "commuter_rail",
    401: "subway",
    700: "bus",
    900: "local_rail",
    1000: "ferry",
}


def transform_routes(routes_df: DataFrame) -> DataFrame:
    """
    Transform raw GTFS routes into the Silver routes model.

    Grain:
        One row per Stockholm-region route.
    """

    transport_mode_expr = F.create_map(
        *[
            item
            for route_type, mode in TRANSPORT_MODE_MAP.items()
            for item in (F.lit(route_type), F.lit(mode))
        ]
    )

    return (
        routes_df
        .filter(
            F.col("agency_id").isin(STOCKHOLM_AGENCY_IDS)
        )
        .select(
            "route_id",
            "route_short_name",
            "route_long_name",
            "route_type",
        )
        .withColumn(
            "transport_mode",
            F.coalesce(
                transport_mode_expr[F.col("route_type")],
                F.lit("unknown"),
            ),
        )
    )


def transform_trips(
    trips_df: DataFrame,
    silver_routes_df: DataFrame,
) -> DataFrame:
    """
    Transform raw GTFS trips into the Silver trips model.

    Grain:
        One row per Stockholm-region trip.
    """

    return (
        trips_df
        .join(
            silver_routes_df.select("route_id"),
            on="route_id",
            how="inner",
        )
        .select(
            "trip_id",
            "route_id",
            "service_id",
            "trip_headsign",
            "direction_id",
            "shape_id",
        )
    )


def gtfs_time_to_seconds(column_name: str):
    """
    Convert a GTFS time string (HH:MM:SS) to seconds
    from service-day midnight.

    Supports GTFS hours >= 24.

    Example:
        25:30:00 -> 91800
    """

    parts = F.split(F.col(column_name), ":")

    return (
        parts[0].cast("int") * 3600
        + parts[1].cast("int") * 60
        + parts[2].cast("int")
    )


def transform_stop_events(
    stop_times_df: DataFrame,
    silver_trips_df: DataFrame,
) -> DataFrame:
    """
    Transform raw GTFS stop_times into the Silver stop events model.

    Grain:
        One row per scheduled stop on a Stockholm-region trip.
    """

    return (
        stop_times_df
        .join(
            silver_trips_df.select("trip_id"),
            on="trip_id",
            how="inner",
        )
        .select(
            "trip_id",
            "stop_sequence",
            "stop_id",
            F.col("arrival_time").alias("arrival_time_raw"),
            F.col("departure_time").alias("departure_time_raw"),
        )
        .withColumn(
            "arrival_seconds",
            gtfs_time_to_seconds("arrival_time_raw"),
        )
        .withColumn(
            "departure_seconds",
            gtfs_time_to_seconds("departure_time_raw"),
        )
    )


def transform_stops(
    stops_df: DataFrame,
    silver_stop_events_df: DataFrame,
) -> DataFrame:
    """
    Transform raw GTFS stops into the Silver stops model.

    Grain:
        One row per GTFS stop/location used by
        Stockholm-region trips.
    """

    used_stop_ids_df = (
        silver_stop_events_df
        .select("stop_id")
        .distinct()
    )

    return (
        stops_df
        .join(
            used_stop_ids_df,
            on="stop_id",
            how="inner",
        )
        .select(
            "stop_id",
            "stop_name",
            "stop_lat",
            "stop_lon",
            "location_type",
            "parent_station",
            "platform_code",
        )
    )