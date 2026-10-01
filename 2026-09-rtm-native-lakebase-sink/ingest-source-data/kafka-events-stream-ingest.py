# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE EXTERNAL VOLUME IF NOT EXISTS <CATALOG>.<SCHEMA>.write_to_lakebase
# MAGIC     LOCATION '<YOUR_EXTERNAL_LOCATION>'

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get("<KAFKA_SECRET_SCOPE>", "<KAFKA_BOOTSTRAP_SECRET_KEY>")
events_topic = 'engagement_events'
volume_path = '/Volumes/<CATALOG>/<SCHEMA>/write_to_lakebase'
checkpoint_path = f'{volume_path}/{events_topic}'

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

events_stream_df = (
  spark.readStream
  .format("delta")
  .option("maxFilesPerTrigger", 1)
  .table("<CATALOG>.<SCHEMA>.engagement_events_stream")
  .withColumn("all_columns", F.to_json(F.struct('event_id', 'user_id', 'video_id', 'event_type', 'device', 'genre', 'watch_seconds', 'timestamp')))
  .selectExpr('CAST(all_columns AS BINARY) AS value')
)

# COMMAND ----------

(
  events_stream_df
  .writeStream
  .queryName('engagement_events_stream')
  .format('kafka')
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
  .option("topic", events_topic)
  .option("checkpointLocation", checkpoint_path)
  .trigger(processingTime = '1 seconds')
  .start()
)

# COMMAND ----------

