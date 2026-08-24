from databricks.connect import DatabricksSession
from pyspark.sql import functions as F


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

CATALOG = "dbw_stockholm_transit_dev"
SCHEMA = "silver"

TABLES = [
    "routes",
    "trips",
    "stop_events",
    "stops",
]

NULL_CHECKS = {
    "routes": ["route_id"],
    "trips": ["trip_id", "route_id"],
    "stop_events": ["trip_id", "stop_id", "stop_sequence"],
    "stops": ["stop_id"],
}


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def main():

    spark = DatabricksSession.builder.getOrCreate()

    print("=" * 60)
    print("STOCKHOLM TRANSIT — SILVER VALIDATION")
    print("=" * 60)

    # ------------------------------------------------------------------
    # Row counts
    # ------------------------------------------------------------------

    print("\nROW COUNTS")
    print("-" * 60)

    for table in TABLES:
        full_table_name = f"{CATALOG}.{SCHEMA}.{table}"

        df = spark.table(full_table_name)
        row_count = df.count()

        print(f"{table:<15} {row_count:>12,} rows")

    # ------------------------------------------------------------------
    # Critical null checks
    # ------------------------------------------------------------------

    print("\nCRITICAL NULL CHECKS")
    print("-" * 60)

    for table, columns in NULL_CHECKS.items():

        df = spark.table(
            f"{CATALOG}.{SCHEMA}.{table}"
        )

        expressions = [
            F.sum(
                F.when(F.col(column).isNull(), 1).otherwise(0)
            ).alias(column)
            for column in columns
        ]

        result = df.agg(*expressions).collect()[0]

        for column in columns:
            null_count = result[column]

            print(
                f"{table}.{column:<25} "
                f"{null_count:>10,} nulls"
            )


    # ------------------------------------------------------------------
    # Uniqueness checks
    # ------------------------------------------------------------------

    print("\nUNIQUENESS CHECKS")
    print("-" * 60)

    uniqueness_checks = {
        "routes": ["route_id"],
        "trips": ["trip_id"],
        "stops": ["stop_id"],
        "stop_events": ["trip_id", "stop_sequence"],
    }

    for table, key_columns in uniqueness_checks.items():

        df = spark.table(
            f"{CATALOG}.{SCHEMA}.{table}"
        )

        duplicate_groups = (
            df
            .groupBy(*key_columns)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        key_name = " + ".join(key_columns)

        print(
            f"{table}.{key_name:<30} "
            f"{duplicate_groups:>10,} duplicate groups"
        )

    # ------------------------------------------------------------------
    # Referential integrity checks
    # ------------------------------------------------------------------

    print("\nREFERENTIAL INTEGRITY CHECKS")
    print("-" * 60)

    routes_df = spark.table(
        f"{CATALOG}.{SCHEMA}.routes"
    )

    trips_df = spark.table(
        f"{CATALOG}.{SCHEMA}.trips"
    )

    stop_events_df = spark.table(
        f"{CATALOG}.{SCHEMA}.stop_events"
    )

    stops_df = spark.table(
        f"{CATALOG}.{SCHEMA}.stops"
    )

    orphan_trip_routes = (
        trips_df
        .select("route_id")
        .distinct()
        .join(
            routes_df.select("route_id").distinct(),
            on="route_id",
            how="left_anti",
        )
        .count()
    )

    orphan_event_trips = (
        stop_events_df
        .select("trip_id")
        .distinct()
        .join(
            trips_df.select("trip_id").distinct(),
            on="trip_id",
            how="left_anti",
        )
        .count()
    )

    orphan_event_stops = (
        stop_events_df
        .select("stop_id")
        .distinct()
        .join(
            stops_df.select("stop_id").distinct(),
            on="stop_id",
            how="left_anti",
        )
        .count()
    )

    print(
        f"trips.route_id -> routes.route_id "
        f"{orphan_trip_routes:>10,} orphan keys"
    )

    print(
        f"stop_events.trip_id -> trips.trip_id "
        f"{orphan_event_trips:>10,} orphan keys"
    )

    print(
        f"stop_events.stop_id -> stops.stop_id "
        f"{orphan_event_stops:>10,} orphan keys"
    )

    # ------------------------------------------------------------------
    # Stop sequence checks
    # ------------------------------------------------------------------

    print("\nSTOP SEQUENCE CHECKS")
    print("-" * 60)

    sequence_profile = (
        stop_events_df
        .groupBy("trip_id")
        .agg(
            F.count("*").alias("stop_count"),
            F.min("stop_sequence").alias("min_sequence"),
            F.max("stop_sequence").alias("max_sequence"),
        )
    )

    sequence_issues = (
        sequence_profile
        .filter(
            (F.col("min_sequence") != 1)
            | (F.col("stop_count") != F.col("max_sequence"))
        )
        .count()
    )

    print(
        f"Trips with sequence issues "
        f"{sequence_issues:>10,}"
    )

    print("\n" + "=" * 60)
    print("VALIDATION COMPLETE")
    print("=" * 60)

if __name__ == "__main__":
    main()