# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS <CATALOG>.<SCHEMA>.write_to_kafka
# MAGIC LOCATION 's3://<YOUR_EXTERNAL_LOCATION>/ad_click_write_to_kafka/'

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get("<KAFKA_SECRET_SCOPE>", "<KAFKA_BOOTSTRAP_SECRET_KEY>")
impressions_topic = 'ad_impressions'
volume_path = '/Volumes/<CATALOG>/<SCHEMA>/write_to_kafka'
checkpoint_path = f'{volume_path}/{impressions_topic}'

dbutils.widgets.text('clean_checkpoint', 'yes')
clean_checkpoint = dbutils.widgets.get('clean_checkpoint')
if clean_checkpoint == 'yes':
    dbutils.fs.rm(checkpoint_path, True)

# COMMAND ----------

class MyStreamingListener(StreamingQueryListener):
    def onQueryStarted(self, event):
        print(f"'{event.name}' [{event.id}] got started!")
    def onQueryProgress(self, event):
        row = event.progress
        print(f"****************************************** batchId ***********************")
        print(f"batchId = {row.batchId} timestamp = {row.timestamp} numInputRows = {row.numInputRows} batchDuration = {row.batchDuration}")
    def onQueryTerminated(self, event):
        print(f"{event.id} got terminated!")

try:
    spark.streams.removeListener(MyStreamingListener())
except:
    pass
spark.streams.addListener(MyStreamingListener())

# COMMAND ----------

impressions_stream_df = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 1)  # 1 file per trigger = 1 second's worth of data from the generator
    .table("<CATALOG>.<SCHEMA>.impressions_stream")
    .withColumn("all_columns", F.to_json(F.struct(
        'impression_id', 'ad_id', 'campaign_id', 'advertiser_id', 
        'publisher_id', 'placement_id', 'device_id', 'device_type', 
        'os', 'geo_country', 'geo_city', 'bid_price_usd', 'impression_time'
    )))
    .selectExpr('CAST(impression_id AS BINARY) AS key', 'CAST(all_columns AS BINARY) AS value') 
)

# COMMAND ----------

(
    impressions_stream_df
    .writeStream
    .queryName('ad_impressions_ingest')
    .format('kafka')
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
    .option("topic", impressions_topic)
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime = '1 seconds')
    .start()
)

# COMMAND ----------

