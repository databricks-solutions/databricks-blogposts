# Bronze: Auto Loader JSON -> append-only streaming tables.
# Every row in a file is one version of a record; updates arrive as new rows with a
# larger updated_at (silver AUTO CDC collapses them).

from pyspark import pipelines as dp
from pyspark.sql import functions as F

raw_path = spark.conf.get("pipeline.raw_path")

LEG_COLS = [
    "leg_id", "shipment_id", "payer_id", "booking_ref", "carrier_ref", "ship_date",
    "consignee_account", "consignee_branch", "service_level", "shipper_name",
    "declared_value", "currency", "is_priority", "updated_at",
]
EVENT_COLS = [
    "leg_id", "event_time", "shipment_id", "location", "milestone", "status_code", "updated_at",
]


def auto_load(subdir, cols):
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(", ".join(f"{c} STRING" for c in cols))
        .load(f"{raw_path}/{subdir}/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
    )


@dp.table(name="shipment_legs_raw", comment="Raw shipment leg versions (append-only).")
def shipment_legs_raw():
    return auto_load("legs", LEG_COLS)


@dp.table(name="tracking_events_raw", comment="Raw tracking event versions (append-only).")
def tracking_events_raw():
    return auto_load("events", EVENT_COLS)
