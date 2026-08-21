# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS <CATALOG>.<SCHEMA>.write_to_kafka
# MAGIC LOCATION 's3://<YOUR_EXTERNAL_LOCATION>/ad_click_write_to_kafka/'

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get("<KAFKA_SECRET_SCOPE>", "<KAFKA_BOOTSTRAP_SECRET_KEY>")
clicks_topic = 'ad_clicks'
volume_path = '/Volumes/<CATALOG>/<SCHEMA>/write_to_kafka'
checkpoint_path = f'{volume_path}/{clicks_topic}'

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

clicks_stream_df = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 1)  # 1 file per trigger = 1 second's worth of data
    .table("<CATALOG>.<SCHEMA>.clicks_stream")
    .withColumn("all_columns", F.to_json(F.struct(
        'click_id', 'impression_id', 'device_id', 'click_time'
    )))
    .selectExpr('CAST(impression_id AS BINARY) AS key', 'CAST(all_columns AS BINARY) AS value') 
)

# COMMAND ----------

(
    clicks_stream_df
    .writeStream
    .queryName('ad_clicks_ingest')
    .format('kafka')
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
    .option("topic", clicks_topic)
    .option("checkpointLocation", checkpoint_path)
    .trigger(processingTime = '1 seconds')
    .start()
)

# COMMAND ----------

