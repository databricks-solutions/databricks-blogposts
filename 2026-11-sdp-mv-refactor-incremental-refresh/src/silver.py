# Silver: current version of each leg and each tracking event (AUTO CDC SCD1), plus one
# row per shipment. CDF + row tracking on every table: the summary reads tracking_events'
# change feed, and the MVs need both on their sources to refresh incrementally.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

cdf_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableRowTracking": "true",
}

dp.create_streaming_table(
    name="shipment_legs",
    comment="Current version of each shipment leg (SCD1).",
    table_properties=cdf_properties,
    cluster_by=["leg_id"],
)
dp.create_auto_cdc_flow(
    target="shipment_legs",
    source="shipment_legs_raw",
    keys=["leg_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
)

dp.create_streaming_table(
    name="tracking_events",
    comment="Current version of each tracking event (SCD1), keyed by (leg_id, event_time).",
    table_properties=cdf_properties,
    cluster_by=["leg_id"],
)
dp.create_auto_cdc_flow(
    target="tracking_events",
    source="tracking_events_raw",
    keys=["leg_id", "event_time"],
    sequence_by="updated_at",
    stored_as_scd_type=1,
)


# shipments is derived from shipment_legs, which receives MERGEs (inserts + updates), so
# read its change feed: inserts and post-update images.
@dp.temporary_view()
def shipments_source():
    return (
        spark.readStream.option("readChangeFeed", "true").table("shipment_legs")
        .filter("_change_type IN ('insert', 'update_postimage')")
        .select(
            "shipment_id",
            "leg_id",
            F.when(F.col("is_priority") == "Y", "Y").otherwise("N").alias("priority_flag"),
            F.lit("N").alias("exception_flag"),
            "updated_at",
        )
    )


dp.create_streaming_table(
    name="shipments",
    comment="One row per shipment.",
    table_properties=cdf_properties,
    cluster_by=["shipment_id"],
)
dp.create_auto_cdc_flow(
    target="shipments",
    source="shipments_source",
    keys=["shipment_id"],
    # Several legs of one shipment can change in the same commit; leg_id breaks the tie.
    sequence_by=F.struct("updated_at", "leg_id"),
    stored_as_scd_type=1,
    except_column_list=["leg_id"],
)
