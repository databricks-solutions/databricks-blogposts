# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

# COMMAND ----------

# MAGIC %sql
# MAGIC  CREATE EXTERNAL VOLUME IF NOT EXISTS gaming_sessionization_demo.rtm_workload.write_to_kafka
# MAGIC     LOCATION 's3://YOUR_EXTERNAL_LOCATION/gaming-sessionization-rtm/write_to_kafka/'

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get("gaming-sessionization-rtm-demo", "kafka-bootstrap-servers")
pc_sessions_topic = 'pc_sessions'
volume_path = '/Volumes/gaming_sessionization_demo/rtm_workload/write_to_kafka'
checkpoint_path = f'{volume_path}/{pc_sessions_topic}'

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

pc_stream_df = (
  spark.readStream
  .format("delta")
  .option("maxFilesPerTrigger", 1)
  .table("gaming_sessionization_demo.rtm_workload.pc_sessions_stream")
  .withColumn("all_columns", F.to_json(F.struct('appSessionId', 'eventId', 'hostPcId', 'psnAccountId', 'timestamp', 'totalFgTime')))
  .selectExpr('CAST(all_columns AS BINARY) AS value') 
)

# COMMAND ----------

(
  pc_stream_df
  .writeStream
  .queryName('pc_sessions_stream')
  .format('kafka')
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
  .option("topic", pc_sessions_topic)
  .option("checkpointLocation", checkpoint_path)
  .trigger(processingTime = '1 seconds')
  .start()
)

# COMMAND ----------

