import os

from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from transformations.transform_realtime import (
    flatten_trip_updates,
    create_realtime_stop_events_df,
    transform_realtime_stop_events,
)


# --------------------------------------------------
# Environment
# --------------------------------------------------

load_dotenv()


# --------------------------------------------------
# Config
# --------------------------------------------------

CATALOG = "dbw_stockholm_transit_dev"
SILVER_SCHEMA = "silver"

REALTIME_TABLE = (
    f"{CATALOG}.{SILVER_SCHEMA}.realtime_stop_events"
)

REALTIME_BASE_PATH = (
    "abfss://raw@ststockholmtransitdev.dfs.core.windows.net/"
    "trafiklab/gtfs_realtime/trip_updates"
)


# --------------------------------------------------
# Spark session
# --------------------------------------------------

def get_spark_session():
    """
    Create the correct Spark session depending on where
    the script is running.

    Databricks Job:
        Use the native SparkSession provided by Databricks.

    Local / Airflow:
        Use Databricks Connect.
    """

    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        print("Running inside Databricks")
        return SparkSession.builder.getOrCreate()

    print("Running through Databricks Connect")

    from databricks.connect import DatabricksSession

    return DatabricksSession.builder.getOrCreate()


# --------------------------------------------------
# Find latest Raw realtime snapshot
# --------------------------------------------------

def get_latest_realtime_snapshot(
    spark,
) -> tuple[str, bytes]:
    """
    Find and load the latest GTFS-Realtime protobuf
    snapshot from the ADLS Raw zone.

    Raw layout:

        ingestion_date=YYYY-MM-DD/
        hour=HH/
        trip_updates_HHMMSS.pb

    Returns:
        tuple[path, protobuf bytes]
    """

    snapshot_pattern = (
        f"{REALTIME_BASE_PATH}/"
        "ingestion_date=*/"
        "hour=*/"
        "*.pb"
    )

    snapshots_df = (
        spark.read
        .format("binaryFile")
        .load(snapshot_pattern)
        .select(
            "path",
            "content",
        )
    )

    latest_snapshot = (
        snapshots_df
        .orderBy(
            F.desc("path")
        )
        .first()
    )

    if latest_snapshot is None:
        raise FileNotFoundError(
            "No GTFS-Realtime protobuf snapshots "
            "were found in ADLS Raw."
        )

    snapshot_path = latest_snapshot["path"]
    snapshot_bytes = bytes(
        latest_snapshot["content"]
    )

    return (
        snapshot_path,
        snapshot_bytes,
    )


# --------------------------------------------------
# Idempotency
# --------------------------------------------------

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

    if not spark.catalog.tableExists(
        REALTIME_TABLE
    ):
        return False

    existing_snapshot = (
        spark.table(REALTIME_TABLE)
        .filter(
            F.col("feed_timestamp")
            == F.lit(feed_timestamp)
        )
        .limit(1)
        .count()
    )

    return existing_snapshot > 0


# --------------------------------------------------
# Delta write
# --------------------------------------------------

def append_delta_table(df) -> None:
    """
    Append a new realtime snapshot to the historical
    Silver Delta table.
    """

    print(
        f"Appending: {REALTIME_TABLE}"
    )

    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(
            REALTIME_TABLE
        )
    )


# --------------------------------------------------
# Main
# --------------------------------------------------

def main() -> None:

    print("=" * 60)
    print(
        "STOCKHOLM TRANSIT — "
        "REALTIME SILVER PIPELINE"
    )
    print("=" * 60)

    # --------------------------------------------------
    # Connect to Spark
    # --------------------------------------------------

    spark = get_spark_session()

    print("Connected to Spark")

    # --------------------------------------------------
    # Load latest Raw snapshot from ADLS
    # --------------------------------------------------

    print(
        "\nFinding latest GTFS-Realtime "
        "snapshot in ADLS..."
    )

    (
        snapshot_path,
        snapshot_bytes,
    ) = get_latest_realtime_snapshot(
        spark
    )

    print(
        f"Latest snapshot: {snapshot_path}"
    )

    print(
        f"Snapshot size: "
        f"{len(snapshot_bytes) / 1024:.2f} KB"
    )

    # --------------------------------------------------
    # Decode protobuf
    # --------------------------------------------------

    print(
        "\nDecoding GTFS-Realtime snapshot..."
    )

    feed = (
        gtfs_realtime_pb2.FeedMessage()
    )

    feed.ParseFromString(
        snapshot_bytes
    )

    print(
        f"Feed entities: "
        f"{len(feed.entity):,}"
    )

    if not feed.header.timestamp:
        raise ValueError(
            "GTFS-Realtime feed is missing "
            "header timestamp."
        )

    # --------------------------------------------------
    # Flatten protobuf
    # --------------------------------------------------

    rows = flatten_trip_updates(
        feed
    )

    print(
        f"Flattened stop observations: "
        f"{len(rows):,}"
    )

    if not rows:
        raise ValueError(
            "GTFS-Realtime snapshot contains "
            "no usable stop observations."
        )

    # --------------------------------------------------
    # Create typed realtime DataFrame
    # --------------------------------------------------

    realtime_df = (
        create_realtime_stop_events_df(
            spark,
            rows,
        )
    )

    # --------------------------------------------------
    # Load validated Static Silver scope
    # --------------------------------------------------

    silver_trips_df = spark.table(
        f"{CATALOG}."
        f"{SILVER_SCHEMA}."
        "trips"
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
        .select(
            "feed_timestamp"
        )
        .first()[
            "feed_timestamp"
        ]
    )

    print(
        f"Snapshot timestamp: "
        f"{snapshot_timestamp}"
    )

    # --------------------------------------------------
    # Idempotency check
    # --------------------------------------------------

    print(
        "\nChecking snapshot history..."
    )

    if snapshot_already_exists(
        spark,
        snapshot_timestamp,
    ):
        print(
            "Snapshot already exists "
            "in Silver."
        )

        print(
            "Skipping append."
        )

        print(
            "\n" + "=" * 60
        )

        print(
            "REALTIME SILVER PIPELINE "
            "COMPLETE — NO CHANGES"
        )

        print(
            "=" * 60
        )

        return

    # --------------------------------------------------
    # Append new snapshot
    # --------------------------------------------------

    snapshot_rows = (
        realtime_stop_events_df
        .count()
    )

    print(
        f"New snapshot contains "
        f"{snapshot_rows:,} "
        "Silver observations"
    )

    print(
        "\nWriting Realtime Silver..."
    )

    append_delta_table(
        realtime_stop_events_df
    )

    # --------------------------------------------------
    # Done
    # --------------------------------------------------

    print(
        "\n" + "=" * 60
    )

    print(
        "REALTIME SILVER PIPELINE COMPLETE"
    )

    print(
        "=" * 60
    )

    print(
        f"Source snapshot: "
        f"{snapshot_path}"
    )

    print(
        f"Appended snapshot: "
        f"{snapshot_timestamp}"
    )

    print(
        f"Rows appended: "
        f"{snapshot_rows:,}"
    )


if __name__ == "__main__":
    main()