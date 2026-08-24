from pathlib import Path

from databricks.connect import DatabricksSession
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from pyspark.sql import functions as F

from transformations.transform_realtime import (
    flatten_trip_updates,
    create_realtime_stop_events_df,
    transform_realtime_stop_events,
)

load_dotenv()

CATALOG = "dbw_stockholm_transit_dev"
SILVER_SCHEMA = "silver"

REALTIME_TABLE = (
    f"{CATALOG}.{SILVER_SCHEMA}.realtime_stop_events"
)

REALTIME_FILE = Path(
    "data/raw/gtfs_realtime/TripUpdatesSweden.pb"
)


def snapshot_already_exists(
    spark,
    feed_timestamp,
) -> bool:
    """
    Check whether a realtime snapshot has already been
    written to Silver.

    This makes repeated processing of the same GTFS-RT
    snapshot idempotent.
    """

    if not spark.catalog.tableExists(REALTIME_TABLE):
        return False

    existing_snapshot = (
        spark.table(REALTIME_TABLE)
        .filter(
            F.col("feed_timestamp") == F.lit(feed_timestamp)
        )
        .limit(1)
        .count()
    )

    return existing_snapshot > 0


def append_delta_table(df) -> None:
    """
    Append a new realtime snapshot to the historical
    Silver Delta table.
    """

    print(f"Appending: {REALTIME_TABLE}")

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(REALTIME_TABLE)
    )


def main() -> None:

    print("=" * 60)
    print("STOCKHOLM TRANSIT — REALTIME SILVER PIPELINE")
    print("=" * 60)

    # --------------------------------------------------
    # Decode protobuf
    # --------------------------------------------------

    print("\nReading GTFS-Realtime snapshot...")

    feed = gtfs_realtime_pb2.FeedMessage()

    with open(REALTIME_FILE, "rb") as file:
        feed.ParseFromString(file.read())

    print(f"Feed entities: {len(feed.entity):,}")

    if not feed.header.timestamp:
        raise ValueError(
            "GTFS-Realtime feed is missing header timestamp."
        )

    # --------------------------------------------------
    # Flatten protobuf
    # --------------------------------------------------

    rows = flatten_trip_updates(feed)

    print(f"Flattened stop observations: {len(rows):,}")

    if not rows:
        raise ValueError(
            "GTFS-Realtime snapshot contains no usable "
            "stop observations."
        )

    # --------------------------------------------------
    # Connect to Databricks
    # --------------------------------------------------

    spark = DatabricksSession.builder.getOrCreate()

    print("Connected to Databricks Spark")

    # --------------------------------------------------
    # Create typed realtime DataFrame
    # --------------------------------------------------

    realtime_df = create_realtime_stop_events_df(
        spark,
        rows,
    )

    # --------------------------------------------------
    # Load Static Silver scope
    # --------------------------------------------------

    silver_trips_df = spark.table(
        f"{CATALOG}.{SILVER_SCHEMA}.trips"
    )

    # --------------------------------------------------
    # Transform Realtime Silver
    # --------------------------------------------------

    realtime_stop_events_df = (
        transform_realtime_stop_events(
            realtime_df,
            silver_trips_df,
        )
    )

    # --------------------------------------------------
    # Get snapshot timestamp
    # --------------------------------------------------

    snapshot_timestamp = (
        realtime_stop_events_df
        .select("feed_timestamp")
        .first()["feed_timestamp"]
    )

    print(
        f"Snapshot timestamp: {snapshot_timestamp}"
    )

    # --------------------------------------------------
    # Idempotency check
    # --------------------------------------------------

    print("\nChecking snapshot history...")

    if snapshot_already_exists(
        spark,
        snapshot_timestamp,
    ):
        print(
            "Snapshot already exists in Silver."
        )
        print("Skipping append.")

        print("\n" + "=" * 60)
        print("REALTIME SILVER PIPELINE COMPLETE — NO CHANGES")
        print("=" * 60)

        return

    # --------------------------------------------------
    # Append new snapshot
    # --------------------------------------------------

    snapshot_rows = realtime_stop_events_df.count()

    print(
        f"New snapshot contains "
        f"{snapshot_rows:,} Silver observations"
    )

    print("\nWriting Realtime Silver...")

    append_delta_table(
        realtime_stop_events_df
    )

    # --------------------------------------------------
    # Done
    # --------------------------------------------------

    print("\n" + "=" * 60)
    print("REALTIME SILVER PIPELINE COMPLETE")
    print("=" * 60)

    print(
        f"Appended snapshot: {snapshot_timestamp}"
    )

    print(
        f"Rows appended: {snapshot_rows:,}"
    )


if __name__ == "__main__":
    main()