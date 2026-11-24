# The refactor: same output as shipment_summary_naive, without windows in any MV.
#   tracking_events CDF -> event_changes (temporary view)
#     AUTO CDC SCD1     -> shipment_first_event / shipment_latest_event (one row per shipment)
#   staged MVs (<= 2 joins each):
#     latest_enriched = shipment_latest_event ⋈ shipment_legs
#     first_enriched  = shipment_first_event  ⋈ shipment_legs
#     shipment_summary = shipments ⋈ latest_enriched ⋈ first_enriched
#   shipment_summary_flat: the same joins in a single MV (4 joins), to compare refresh techniques.

from pyspark import pipelines as dp
from pyspark.sql import functions as F

cdf_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.enableRowTracking": "true",
}


# -----------------------------------------------------------------------------
# 1) Events from tracking_events' change feed: inserts and post-update images only
#    (the current version of every new or changed event).
# -----------------------------------------------------------------------------
@dp.temporary_view()
@dp.expect_or_drop("valid_shipment", "shipment_id IS NOT NULL")
@dp.expect_or_drop("valid_event_ts", "event_ts IS NOT NULL")
def event_changes():
    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table("tracking_events")
        .filter("_change_type IN ('insert', 'update_postimage')")
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("updated_ts", F.to_timestamp("updated_at"))
        # SCD1 keeps the greatest sequence; negate the time so the earliest event wins.
        .withColumn("neg_event_us", -F.expr("unix_micros(event_ts)"))
        .select("shipment_id", "leg_id", "event_time", "event_ts", "neg_event_us",
                "updated_ts", "location", "milestone", "status_code")
    )


# -----------------------------------------------------------------------------
# 2) First and latest event per shipment, replacing the two ROW_NUMBER() windows.
#    leg_id breaks ties between legs; updated_ts picks the newest version of an event.
# -----------------------------------------------------------------------------
for name, order_key, what in [
    ("shipment_latest_event", "event_ts", "Latest"),
    ("shipment_first_event", "neg_event_us", "First"),
]:
    dp.create_streaming_table(
        name=name,
        comment=f"{what} tracking event per shipment.",
        table_properties=cdf_properties,
        cluster_by=["shipment_id"],
    )
    dp.create_auto_cdc_flow(
        target=name,
        source="event_changes",
        keys=["shipment_id"],
        sequence_by=F.struct(order_key, "leg_id", "updated_ts"),
        stored_as_scd_type=1,
    )


# -----------------------------------------------------------------------------
# 3) Staged joins. The leg row is looked up by the winning event's leg_id, so leg-only
#    updates (e.g. a new consignee account) flow through on refresh.
# -----------------------------------------------------------------------------
LATEST_COLS = ["location", "milestone", "status_code"]
LATEST_LEG_COLS = ["payer_id", "carrier_ref", "consignee_account", "consignee_branch",
                   "shipper_name", "declared_value", "currency"]
FIRST_LEG_COLS = ["booking_ref", "ship_date", "service_level"]
OUTPUT_COLS = ["shipment_id", "exception_flag", "location", "milestone", "status_code",
               "payer_id", "booking_ref", "carrier_ref", "ship_date", "consignee_account",
               "consignee_branch", "service_level", "shipper_name", "declared_value",
               "currency"]


def with_leg(event_table, event_cols, leg_cols):
    ev = spark.read.table(event_table).alias("ev")
    leg = spark.read.table("shipment_legs").alias("leg")
    return (
        ev.join(leg, (F.col("leg.leg_id") == F.col("ev.leg_id"))
                     & (F.col("leg.shipment_id") == F.col("ev.shipment_id")))
        .select(F.col("ev.shipment_id"),
                *[F.col(f"ev.{c}") for c in event_cols],
                *[F.col(f"leg.{c}") for c in leg_cols])
    )


@dp.materialized_view(name="latest_enriched", cluster_by=["shipment_id"],
                      comment="Latest event per shipment + its leg row (1 join).")
def latest_enriched():
    return with_leg("shipment_latest_event", LATEST_COLS, LATEST_LEG_COLS)


@dp.materialized_view(name="first_enriched", cluster_by=["shipment_id"],
                      comment="First event per shipment + its leg row (1 join).")
def first_enriched():
    return with_leg("shipment_first_event", [], FIRST_LEG_COLS)


@dp.materialized_view(name="shipment_summary", cluster_by=["shipment_id"],
                      comment="One row per shipment: window-free, staged replacement (2 joins).")
def shipment_summary():
    s = spark.read.table("shipments").alias("s")
    le = spark.read.table("latest_enriched").alias("le")
    fe = spark.read.table("first_enriched").alias("fe")
    return (
        s.join(le, "shipment_id").join(fe, "shipment_id")
        .select("shipment_id", "s.exception_flag",
                *[F.col(f"le.{c}") for c in LATEST_COLS + LATEST_LEG_COLS],
                *[F.col(f"fe.{c}") for c in FIRST_LEG_COLS])
        .select(*OUTPUT_COLS)
    )


# -----------------------------------------------------------------------------
# 4) For comparison only: window-free, but all four joins in one MV.
# -----------------------------------------------------------------------------
@dp.materialized_view(name="shipment_summary_flat",
                      comment="Comparison: window-free, but 4 joins in a single MV.")
def shipment_summary_flat():
    s = spark.read.table("shipments").alias("s")
    le = spark.read.table("shipment_latest_event").alias("le")
    fe = spark.read.table("shipment_first_event").alias("fe")
    ll = spark.read.table("shipment_legs").alias("ll")
    fl = spark.read.table("shipment_legs").alias("fl")
    return (
        s.join(le, F.col("le.shipment_id") == F.col("s.shipment_id"))
        .join(ll, (F.col("ll.leg_id") == F.col("le.leg_id"))
                  & (F.col("ll.shipment_id") == F.col("le.shipment_id")))
        .join(fe, F.col("fe.shipment_id") == F.col("s.shipment_id"))
        .join(fl, (F.col("fl.leg_id") == F.col("fe.leg_id"))
                  & (F.col("fl.shipment_id") == F.col("fe.shipment_id")))
        .select(F.col("s.shipment_id"), F.col("s.exception_flag"),
                *[F.col(f"le.{c}") for c in LATEST_COLS],
                *[F.col(f"ll.{c}") for c in LATEST_LEG_COLS],
                *[F.col(f"fl.{c}") for c in FIRST_LEG_COLS])
        .select(*OUTPUT_COLS)
    )
