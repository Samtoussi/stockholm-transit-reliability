import base64

from cryptography.hazmat.primitives import serialization
from pyspark.sql import SparkSession


CATALOG = "dbw_stockholm_transit_dev"
SOURCE_SCHEMA = "gold"

SNOWFLAKE_DATABASE = "STOCKHOLM_TRANSIT"
SNOWFLAKE_SCHEMA = "GOLD"
SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"
SNOWFLAKE_ROLE = "STOCKHOLM_TRANSIT_LOADER"
SNOWFLAKE_USER = "DATABRICKS_LOADER"
SNOWFLAKE_URL = "https://JYKAGJL-PH18045.snowflakecomputing.com"

SECRET_SCOPE = "stockholm-transit"
PRIVATE_KEY_SECRET = "snowflake-private-key"


GOLD_TABLES = [
    "route_reliability",
    "stop_reliability",
    "route_stop_reliability",
    "route_direction_reliability",
    "route_hourly_reliability",
    "route_weekday_reliability",
    "route_delay_propagation",
]


def get_spark_session() -> SparkSession:
    return SparkSession.builder.getOrCreate()


def get_snowflake_options() -> dict:
    private_key_pem = dbutils.secrets.get(
        scope=SECRET_SCOPE,
        key=PRIVATE_KEY_SECRET,
    )

    private_key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=None,
    )

    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    private_key_b64 = base64.b64encode(
        private_key_der
    ).decode("utf-8")

    return {
        "sfURL": SNOWFLAKE_URL,
        "sfUser": SNOWFLAKE_USER,
        "sfDatabase": SNOWFLAKE_DATABASE,
        "sfSchema": SNOWFLAKE_SCHEMA,
        "sfWarehouse": SNOWFLAKE_WAREHOUSE,
        "sfRole": SNOWFLAKE_ROLE,
        "pem_private_key": private_key_b64,
    }


def publish_table(
    spark: SparkSession,
    table_name: str,
    snowflake_options: dict,
) -> None:
    source_table = (
        f"{CATALOG}.{SOURCE_SCHEMA}.{table_name}"
    )

    target_table = table_name.upper()

    print(
        f"Publishing {source_table} "
        f"→ {SNOWFLAKE_DATABASE}."
        f"{SNOWFLAKE_SCHEMA}.{target_table}"
    )

    df = spark.table(source_table)

    (
        df.write
        .format("snowflake")
        .options(**snowflake_options)
        .option("dbtable", target_table)
        .mode("overwrite")
        .save()
    )

    print(
        f"Published {target_table}"
    )


def main() -> None:
    print("=" * 60)
    print("SNOWFLAKE GOLD PUBLISH")
    print("=" * 60)

    spark = get_spark_session()
    snowflake_options = get_snowflake_options()

    for table_name in GOLD_TABLES:
        publish_table(
            spark=spark,
            table_name=table_name,
            snowflake_options=snowflake_options,
        )

    print("=" * 60)
    print("SNOWFLAKE GOLD PUBLISH COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    main()