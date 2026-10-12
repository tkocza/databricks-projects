from pyspark import pipelines as dp
from pyspark.sql import functions as F

from db_flights_lakeflow.shared.metadata import TECHNICAL_COLUMNS

BRONZE = "db_flights_lakeflow.bronze.airports"
SILVER = "db_flights_lakeflow.silver.airports"


@dp.view(name="airports_latest")
@dp.expect_or_fail("airport_code_not_null", "airport_code IS NOT NULL")
@dp.expect("airport_code_valid_length", "length(airport_code) = 3")
def airports_latest():

    bronze = spark.read.table(BRONZE)

    # find the new batch of records
    latest_load = bronze.agg(F.max("load_date").alias("load_date"))

    df = bronze.join(latest_load, "load_date", "left_semi")
    df = df.withColumnsRenamed({"iata_code": "airport_code"})
    df = df.drop(*TECHNICAL_COLUMNS)

    return df


dp.create_streaming_table(SILVER)

dp.create_auto_cdc_from_snapshot_flow(
    target=SILVER,
    source="airports_latest",
    keys=["airport_code"],
    stored_as_scd_type=2,
)