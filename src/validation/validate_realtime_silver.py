from databricks.connect import DatabricksSession
from dotenv import load_dotenv
from pyspark.sql import functions as F

load_dotenv()

CATALOG = "dbw_stockholm_transit_dev"
SCHEMA = "silver"

REALTIME_TABLE = f"{CATALOG}.{SCHEMA}.realtime_stop_events"
TRIPS_TABLE = f"{CATALOG}.{SCHEMA}.trips"
STOPS_TABLE = f"{CATALOG}.{SCHEMA}.stops"


def main():
    spark = DatabricksSession.builder.getOrCreate()

    print("=" * 60)
    print("STOCKHOLM TRANSIT — REALTIME SILVER VALIDATION")
    print("=" * 60)

    realtime_df = spark.table(REALTIME_TABLE)
    trips_df = spark.table(TRIPS_TABLE)
    stops_df = spark.table(STOPS_TABLE)

    # --------------------------------------------------
    # Row count
    # --------------------------------------------------

    print("\nROW COUNT")
    print("-" * 60)

    row_count = realtime_df.count()

    print(f"realtime_stop_events: {row_count:,} rows")

    # --------------------------------------------------
    # Critical null checks
    # --------------------------------------------------

    print("\nCRITICAL NULL CHECKS")
    print("-" * 60)

    critical_columns = [
        "trip_id",
        "stop_id",
        "stop_sequence",
        "feed_timestamp",
        "start_date",
    ]

    expressions = [
        F.sum(
            F.when(F.col(column).isNull(), 1).otherwise(0)
        ).alias(column)
        for column in critical_columns
    ]

    null_result = realtime_df.agg(*expressions).collect()[0]

    for column in critical_columns:
        print(
            f"{column:<30} "
            f"{null_result[column]:>10,} nulls"
        )

    # --------------------------------------------------
    # Observation key uniqueness
    # --------------------------------------------------

    print("\nOBSERVATION KEY UNIQUENESS")
    print("-" * 60)

    duplicate_groups = (
        realtime_df
        .groupBy(
            "feed_timestamp",
            "trip_id",
            "stop_sequence",
        )
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    print(
        "feed_timestamp + trip_id + stop_sequence "
        f"{duplicate_groups:>10,} duplicate groups"
    )

    # --------------------------------------------------
    # Referential integrity
    # --------------------------------------------------

    print("\nREFERENTIAL INTEGRITY CHECKS")
    print("-" * 60)

    orphan_trips = (
        realtime_df
        .select("trip_id")
        .distinct()
        .join(
            trips_df.select("trip_id").distinct(),
            on="trip_id",
            how="left_anti",
        )
        .count()
    )

    orphan_stops = (
        realtime_df
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
        f"realtime.trip_id -> trips.trip_id "
        f"{orphan_trips:>10,} orphan keys"
    )

    print(
        f"realtime.stop_id -> stops.stop_id "
        f"{orphan_stops:>10,} orphan keys"
    )

    # --------------------------------------------------
    # Delay profile
    # --------------------------------------------------

    print("\nDELAY PROFILE")
    print("-" * 60)

    delay_profile = (
        realtime_df
        .agg(
            F.count("arrival_delay_seconds").alias("arrival_count"),
            F.min("arrival_delay_seconds").alias("arrival_min"),
            F.max("arrival_delay_seconds").alias("arrival_max"),
            F.avg("arrival_delay_seconds").alias("arrival_avg"),

            F.count("departure_delay_seconds").alias("departure_count"),
            F.min("departure_delay_seconds").alias("departure_min"),
            F.max("departure_delay_seconds").alias("departure_max"),
            F.avg("departure_delay_seconds").alias("departure_avg"),
        )
        .collect()[0]
    )

    print(
        f"Arrival observations:   "
        f"{delay_profile['arrival_count']:,}"
    )

    print(
        f"Arrival min / avg / max: "
        f"{delay_profile['arrival_min']} / "
        f"{delay_profile['arrival_avg']:.2f} / "
        f"{delay_profile['arrival_max']} seconds"
    )

    print(
        f"Departure observations: "
        f"{delay_profile['departure_count']:,}"
    )

    print(
        f"Departure min / avg / max: "
        f"{delay_profile['departure_min']} / "
        f"{delay_profile['departure_avg']:.2f} / "
        f"{delay_profile['departure_max']} seconds"
    )

    # --------------------------------------------------
    # Feed timestamp profile
    # --------------------------------------------------

    print("\nFEED TIMESTAMP PROFILE")
    print("-" * 60)

    feed_profile = (
        realtime_df
        .agg(
            F.countDistinct("feed_timestamp").alias(
                "unique_feed_timestamps"
            ),
            F.min("feed_timestamp").alias("min_feed_timestamp"),
            F.max("feed_timestamp").alias("max_feed_timestamp"),
        )
        .collect()[0]
    )

    print(
        f"Unique feed timestamps: "
        f"{feed_profile['unique_feed_timestamps']:,}"
    )

    print(
        f"Min feed timestamp: "
        f"{feed_profile['min_feed_timestamp']}"
    )

    print(
        f"Max feed timestamp: "
        f"{feed_profile['max_feed_timestamp']}"
    )

    # --------------------------------------------------
    # Duplicate observation examples
    # --------------------------------------------------

    print("\nDUPLICATE OBSERVATION EXAMPLES")
    print("-" * 60)

    duplicate_observations = (
        realtime_df
        .groupBy(
            "feed_timestamp",
            "trip_id",
            "stop_sequence",
        )
        .count()
        .filter(F.col("count") > 1)
        .orderBy(F.desc("count"))
    )

    duplicate_observations.show(
        20,
        truncate=False,
    )

    # --------------------------------------------------
    # Orphan stop examples
    # --------------------------------------------------

    print("\nORPHAN STOP EXAMPLES")
    print("-" * 60)

    orphan_stop_examples = (
        realtime_df
        .select(
            "trip_id",
            "stop_id",
            "stop_sequence",
            "feed_timestamp",
        )
        .join(
            stops_df.select("stop_id"),
            on="stop_id",
            how="left_anti",
        )
        .distinct()
    )

    orphan_stop_examples.show(
        20,
        truncate=False,
    )

    print("\n" + "=" * 60)
    print("VALIDATION COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()