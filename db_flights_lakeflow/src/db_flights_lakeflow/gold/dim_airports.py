from pyspark import pipelines as dp
from pyspark.sql import functions as F

from db_flights_lakeflow.shared.metadata import SCD_VALID_FROM, SCD_VALID_TO

SILVER = "db_flights_lakeflow.silver.airports"
GOLD = "db_flights_lakeflow.gold.dim_airport"


@dp.materialized_view(
    name=GOLD,
    comment="Airports dimension (SCD2) built from silver.airports. "
            "Includes special records -1 (UNKNOWN) and -999 (QUARANTINED).",
    schema="""
        airport_id BIGINT COMMENT 'Surrogate key for the airports dimension (hash of airport_code and valid_from; -1 and -999 are special records)',
        airport_code STRING NOT NULL COMMENT 'Airport IATA code (3-letter)',
        airport STRING COMMENT 'Full airport name',
        city STRING COMMENT 'City of airport location',
        state STRING COMMENT 'State or region',
        country STRING COMMENT 'Country',
        latitude DOUBLE COMMENT 'Latitude coordinate',
        longitude DOUBLE COMMENT 'Longitude coordinate',
        valid_from TIMESTAMP COMMENT 'Timestamp from which this dimension version is valid',
        valid_to TIMESTAMP COMMENT 'Timestamp until which this dimension version is valid',
        is_current BOOLEAN COMMENT 'Indicates whether this is the current version of the record'
    """,
)
def dim_airport():
    silver = spark.read.table(SILVER)
    initial_rows = silver.agg(F.min("__START_AT").alias("initial_start"))

    dim = (
        silver.crossJoin(initial_rows)
        .select(
            "airport_code",
            "airport",
            "city",
            "state",
            "country",
            "latitude",
            "longitude",
            F.when(
                F.col("__START_AT") == F.col("initial_start"),
                F.lit(SCD_VALID_FROM).cast("timestamp"),
            ).otherwise(F.col("__START_AT")).alias("valid_from"),
            F.coalesce(
                F.col("__END_AT") - F.expr("INTERVAL 1 SECOND"),
                F.lit(SCD_VALID_TO).cast("timestamp"),
            ).alias("valid_to"),
            F.col("__END_AT").isNull().alias("is_current"),
        )
        # positive-only hash
        .withColumn(
            "airport_id",
            F.xxhash64("airport_code", "valid_from").bitwiseAND(F.lit(9223372036854775807)),
        )
    )

    special_records = (
        spark.createDataFrame(
            [
                (-1, "UNKNOWN", "Unknown Airport", None, None, None, None, None),
                (-999, "QUARANTINED", "Missing Airport", None, None, None, None, None),
            ],
            "airport_id long, airport_code string, airport string, city string, state string, "
            "country string, latitude double, longitude double",
        )
        .withColumn("valid_from", F.lit(SCD_VALID_FROM).cast("timestamp"))
        .withColumn("valid_to", F.lit(SCD_VALID_TO).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
    )

    df = dim.unionByName(special_records)
    df = df.select(
        "airport_id", "airport_code", "airport", "city", "state", "country",
        "latitude", "longitude", "valid_from", "valid_to", "is_current",
    )

    return df