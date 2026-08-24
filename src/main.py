from databricks.connect import DatabricksSession

from transformations.transform_static_gtfs import (
    transform_routes,
    transform_trips,
    transform_stop_events,
    transform_stops,
)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CATALOG = "dbw_stockholm_transit_dev"
SILVER_SCHEMA = "silver"

BRONZE_BASE_PATH = (
    "/Volumes/dbw_stockholm_transit_dev/"
    "default/gtfs_work/extracted"
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_gtfs_csv(spark, filename: str):
    """
    Read a GTFS CSV file from the Unity Catalog Volume.
    """

    path = f"{BRONZE_BASE_PATH}/{filename}"

    print(f"Reading: {path}")

    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def write_delta_table(df, table_name: str):
    """
    Write a DataFrame to a managed Silver Delta table.
    """

    full_table_name = f"{CATALOG}.{SILVER_SCHEMA}.{table_name}"

    print(f"Writing: {full_table_name}")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table_name)
    )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def main():

    print("=" * 60)
    print("STOCKHOLM TRANSIT — STATIC GTFS SILVER PIPELINE")
    print("=" * 60)

    # Remote Spark session through Databricks Connect
    spark = DatabricksSession.builder.getOrCreate()

    print("Connected to Databricks Spark")

    # ------------------------------------------------------------------
    # Read GTFS
    # ------------------------------------------------------------------

    print("\nReading GTFS files...")

    routes_raw = read_gtfs_csv(
        spark,
        "routes.txt",
    )

    trips_raw = read_gtfs_csv(
        spark,
        "trips.txt",
    )

    stop_times_raw = read_gtfs_csv(
        spark,
        "stop_times.txt",
    )

    stops_raw = read_gtfs_csv(
        spark,
        "stops.txt",
    )

    # ------------------------------------------------------------------
    # Transform Silver
    # ------------------------------------------------------------------

    print("\nTransforming GTFS data...")

    routes_silver = transform_routes(
        routes_raw,
    )

    trips_silver = transform_trips(
        trips_raw,
        routes_silver,
    )

    stop_events_silver = transform_stop_events(
        stop_times_raw,
        trips_silver,
    )

    stops_silver = transform_stops(
        stops_raw,
        stop_events_silver,
    )

    # ------------------------------------------------------------------
    # Write Silver Delta tables
    # ------------------------------------------------------------------

    print("\nWriting Silver Delta tables...")

    write_delta_table(
        routes_silver,
        "routes",
    )

    write_delta_table(
        trips_silver,
        "trips",
    )

    write_delta_table(
        stop_events_silver,
        "stop_events",
    )

    write_delta_table(
        stops_silver,
        "stops",
    )

    print("\n" + "=" * 60)
    print("SILVER PIPELINE COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()