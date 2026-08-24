from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)


REALTIME_STOP_EVENTS_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), False),
        StructField("stop_id", StringType(), True),
        StructField("stop_sequence", LongType(), True),
        StructField("arrival_time_actual", LongType(), True),
        StructField("departure_time_actual", LongType(), True),
        StructField("arrival_delay_seconds", LongType(), True),
        StructField("departure_delay_seconds", LongType(), True),
        StructField("feed_timestamp", LongType(), True),
        StructField("start_date", StringType(), True),
    ]
)


def flatten_trip_updates(feed) -> list[dict]:
    """
    Flatten GTFS-Realtime TripUpdates into stop-level observations.

    Grain:
        One row per realtime stop observation.
    """

    rows = []

    feed_timestamp = (
        int(feed.header.timestamp)
        if feed.header.timestamp
        else None
    )

    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue

        trip_update = entity.trip_update
        trip = trip_update.trip

        trip_id = trip.trip_id or None
        start_date = trip.start_date or None

        if not trip_id:
            continue

        for stop_update in trip_update.stop_time_update:

            stop_id = stop_update.stop_id or None

            stop_sequence = (
                int(stop_update.stop_sequence)
                if stop_update.HasField("stop_sequence")
                else None
            )

            arrival_time = None
            arrival_delay = None
            departure_time = None
            departure_delay = None

            if stop_update.HasField("arrival"):
                if stop_update.arrival.HasField("time"):
                    arrival_time = int(
                        stop_update.arrival.time
                    )

                if stop_update.arrival.HasField("delay"):
                    arrival_delay = int(
                        stop_update.arrival.delay
                    )

            if stop_update.HasField("departure"):
                if stop_update.departure.HasField("time"):
                    departure_time = int(
                        stop_update.departure.time
                    )

                if stop_update.departure.HasField("delay"):
                    departure_delay = int(
                        stop_update.departure.delay
                    )

            rows.append(
                {
                    "trip_id": trip_id,
                    "stop_id": stop_id,
                    "stop_sequence": stop_sequence,
                    "arrival_time_actual": arrival_time,
                    "departure_time_actual": departure_time,
                    "arrival_delay_seconds": arrival_delay,
                    "departure_delay_seconds": departure_delay,
                    "feed_timestamp": feed_timestamp,
                    "start_date": start_date,
                }
            )

    return rows


def create_realtime_stop_events_df(
    spark: SparkSession,
    rows: list[dict],
) -> DataFrame:
    """
    Create a typed Spark DataFrame from flattened
    GTFS-Realtime observations.
    """

    df = spark.createDataFrame(
        rows,
        schema=REALTIME_STOP_EVENTS_SCHEMA,
    )

    return (
        df
        .withColumn(
            "arrival_time_actual",
            F.timestamp_seconds("arrival_time_actual"),
        )
        .withColumn(
            "departure_time_actual",
            F.timestamp_seconds("departure_time_actual"),
        )
        .withColumn(
            "feed_timestamp",
            F.timestamp_seconds("feed_timestamp"),
        )
        .withColumn(
            "start_date",
            F.to_date("start_date", "yyyyMMdd"),
        )
    )


def transform_realtime_stop_events(
    realtime_df: DataFrame,
    silver_trips_df: DataFrame,
) -> DataFrame:
    """
    Transform GTFS-Realtime observations into the
    Stockholm-region Realtime Silver model.

    Grain:
        One row per realtime stop observation
        per feed snapshot.

    Duplicate observations from duplicate GTFS-RT
    entities are removed using the observation key:

        feed_timestamp + trip_id + stop_sequence
    """

    return (
        realtime_df
        .join(
            silver_trips_df.select("trip_id"),
            on="trip_id",
            how="inner",
        )
        .select(
            "trip_id",
            "stop_id",
            "stop_sequence",
            "arrival_time_actual",
            "departure_time_actual",
            "arrival_delay_seconds",
            "departure_delay_seconds",
            "feed_timestamp",
            "start_date",
        )
        .dropDuplicates(
            [
                "feed_timestamp",
                "trip_id",
                "stop_sequence",
            ]
        )
    )