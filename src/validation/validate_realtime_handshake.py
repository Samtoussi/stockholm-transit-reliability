from pathlib import Path

from databricks.connect import DatabricksSession
from google.transit import gtfs_realtime_pb2
from pyspark.sql import functions as F


# --------------------------------------------------
# Configuration
# --------------------------------------------------

CATALOG = "dbw_stockholm_transit_dev"
SCHEMA = "silver"

REALTIME_FILE = Path(
    "data/raw/gtfs_realtime/TripUpdatesSweden.pb"
)


# --------------------------------------------------
# Validation
# --------------------------------------------------

def main():

    print("=" * 60)
    print("STOCKHOLM TRANSIT — REALTIME ↔ SILVER HANDSHAKE")
    print("=" * 60)

    # --------------------------------------------------
    # Decode local GTFS-Realtime snapshot
    # --------------------------------------------------

    feed = gtfs_realtime_pb2.FeedMessage()

    with open(REALTIME_FILE, "rb") as file:
        feed.ParseFromString(file.read())

    realtime_trip_ids = {
        entity.trip_update.trip.trip_id
        for entity in feed.entity
        if (
            entity.HasField("trip_update")
            and entity.trip_update.trip.trip_id
        )
    }

    print(f"\nRealtime unique trip_ids: {len(realtime_trip_ids):,}")

    # --------------------------------------------------
    # Connect to Databricks
    # --------------------------------------------------

    spark = DatabricksSession.builder.getOrCreate()

    silver_trips = (
        spark
        .table(f"{CATALOG}.{SCHEMA}.trips")
        .select("trip_id")
        .distinct()
    )

    # --------------------------------------------------
    # Create realtime Spark DataFrame
    # --------------------------------------------------

    realtime_trips = spark.createDataFrame(
        [(trip_id,) for trip_id in realtime_trip_ids],
        ["trip_id"],
    )

    # Ensure matching datatype
    realtime_trips = realtime_trips.withColumn(
        "trip_id",
        F.col("trip_id").cast("string"),
    )

    silver_trips = silver_trips.withColumn(
        "trip_id",
        F.col("trip_id").cast("string"),
    )

    # --------------------------------------------------
    # Match
    # --------------------------------------------------

    matched = (
        realtime_trips
        .join(
            silver_trips,
            on="trip_id",
            how="inner",
        )
        .count()
    )

    unmatched = (
        realtime_trips
        .join(
            silver_trips,
            on="trip_id",
            how="left_anti",
        )
        .count()
    )

    total = len(realtime_trip_ids)

    match_rate = (
        matched / total * 100
        if total
        else 0
    )

    # --------------------------------------------------
    # Results
    # --------------------------------------------------

    print("\nREALTIME ↔ SILVER MATCH")
    print("-" * 60)

    print(f"Realtime trip_ids: {total:,}")
    print(f"Matched Silver:    {matched:,}")
    print(f"Unmatched Silver:  {unmatched:,}")
    print(f"Match rate:        {match_rate:.2f}%")

    print("\n" + "=" * 60)
    print("HANDSHAKE VALIDATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()