# Contact cleanup: one micro-batch, two idempotent Delta writes.
#
# Phone numbers are normalized to E.164 with the phonenumbers library. Rows
# that parse cleanly land in a curated table; the rest go to a quarantine
# table for review. Both appends are idempotent: they key txnVersion on the
# ForEachBatch batch_id, so a retried batch is skipped as an already-seen
# duplicate rather than double-written.
#
# Companion code for the Databricks Community blog post
# "One Pipeline, Any Destination: The ForEachBatch Sink in Spark Declarative
# Pipelines is now GA".

import phonenumbers

from pyspark import pipelines as dp
from pyspark.sql import functions as F


@F.udf("string")
def to_e164(raw):
    if not raw:
        return None
    try:
        parsed = phonenumbers.parse(raw, "US")
        return phonenumbers.format_number(
            parsed, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        return None


@dp.foreach_batch_sink(name="contact_cleanup_sink")
def clean_contacts(df, batch_id):
    cleaned = df.withColumn("phone_e164", to_e164("phone_raw")).persist()
    (cleaned.where("phone_e164 IS NOT NULL").write
        .option("txnVersion", batch_id).option("txnAppId", "contact_cleanup_clean")
        .mode("append").saveAsTable("main.crm.contacts_clean"))
    (cleaned.where("phone_e164 IS NULL").write
        .option("txnVersion", batch_id).option("txnAppId", "contact_cleanup_quarantine")
        .mode("append").saveAsTable("main.crm.contacts_quarantine"))
    cleaned.unpersist()


@dp.append_flow(target="contact_cleanup_sink")
def raw_contacts_flow():
    return spark.readStream.table("main.crm.raw_contacts")
