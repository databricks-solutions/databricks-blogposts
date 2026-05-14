# Databricks notebook source
from datetime import datetime, timedelta
import uuid
import random
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog gaming_sessionization_demo;
# MAGIC create schema if not exists rtm_workload;
# MAGIC use schema rtm_workload;
# MAGIC
# MAGIC ALTER SCHEMA rtm_workload DISABLE PREDICTIVE OPTIMIZATION;
# MAGIC -- drop table if exists gaming_sessionization_demo.rtm_workload.pc_sessions;
# MAGIC -- drop table if exists gaming_sessionization_demo.rtm_workload.console_sessions;

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.databricks.delta.autoCompact.enabled = false;
# MAGIC set spark.databricks.delta.optimizeWrite.enabled = false;

# COMMAND ----------

# def get_device_id():
#     return str(uuid.uuid4())

# def get_session_id():
#     return random.randint(1000000000, 9999999999)

# def get_sink():
#     return random.choice(['PC','Console'])

# def get_account_id():
#     return str(uuid.uuid4())[:9] + str(random.randint(999999000000000, 999999999999999))

# def get_session_start_time(start_time, end_time):
#   total_seconds = int((end_time - start_time).total_seconds())
#   return start_time + timedelta(seconds=random.randint(0, total_seconds))

# def get_session_end_time(session_start_time):
#     windows = [
#         (2*60, 4*60, 0.25),   
#         (4*60, 6*60, 0.25),
#         (6*60, 8*60, 0.25),                   
#         (8*60, 10*60, 0.25)        
#     ]
#     window = random.choices(
#         population=windows,
#         weights=[w[2] for w in windows],
#         k=1
#     )[0]
#     offset_seconds = random.randint(window[0], window[1])
#     session_end_time = session_start_time + timedelta(seconds=offset_seconds)
#     return session_end_time, offset_seconds    



# COMMAND ----------

# # Example start and end times (ISO format)
# start_time = datetime.fromisoformat("2025-11-01T00:00:00")
# end_time = datetime.fromisoformat("2025-11-01T00:59:59")
# eventIds = ['ApplicationSessionStart', 'ApplicationSessionEndBi']
# minute_counter = 0 

# minute_start = start_time
# while minute_start < end_time:
#   minute_end = minute_start + timedelta(seconds=59)
#   print(f"Minute: {minute_start} — {minute_end}")

#   pc_session_data = []
#   console_session_data = []

#   if minute_counter < 2:
#     num_sessions = 500000
#   elif 2 <= minute_counter < 3:
#     num_sessions = 475000
#   elif 3 <= minute_counter < 4:
#     num_sessions = 425000
#   elif 4 <= minute_counter < 5:
#     num_sessions = 400000  
#   elif 5 <= minute_counter < 6:
#     num_sessions = 350000        
#   elif 6 <= minute_counter < 8:
#     num_sessions = 275000
#   elif 8 <= minute_counter < 12:
#     num_sessions = 200000
#   elif 12 <= minute_counter < 15:
#     num_sessions = 225000

#   elif 15 <= minute_counter < 19:
#     num_sessions = 300000
#   elif 19 <= minute_counter < 23:
#     num_sessions = 275000
#   elif 23 <= minute_counter < 27:
#     num_sessions = 250000
#   elif 27 <= minute_counter < 30:
#     num_sessions = 225000

#   elif 30 <= minute_counter < 34:
#     num_sessions = 300000
#   elif 34 <= minute_counter < 38:
#     num_sessions = 275000
#   elif 38 <= minute_counter < 42:
#     num_sessions = 250000
#   elif 42 <= minute_counter < 45:
#     num_sessions = 225000

#   elif 45 <= minute_counter < 49:
#     num_sessions = 300000
#   elif 49 <= minute_counter < 53:
#     num_sessions = 275000
#   elif 53 <= minute_counter < 57:
#     num_sessions = 250000
#   elif 57 <= minute_counter < 60:
#     num_sessions = 225000    


#   for _ in range(num_sessions):
#     sink = get_sink()
#     deviceId = get_device_id()
#     sessionId = get_session_id()
#     accountId = get_account_id() #psnAccountId
#     sessionIdStartTime = get_session_start_time(minute_start, minute_end)
#     sessionIdEndTime, sessionDuration = get_session_end_time(sessionIdStartTime)

#     if sink == 'PC':
#       session_start = {
#           "hostPcId": deviceId,
#           "appSessionId": sessionId,
#           "psnAccountId": accountId,
#           "eventId": "ApplicationSessionStart",
#           "timestamp": sessionIdStartTime,
#           "totalFgTime": 0
#       }
#       session_end = {
#           "hostPcId": deviceId,
#           "appSessionId": sessionId,
#           "psnAccountId": accountId,
#           "eventId": "ApplicationSessionEndBi",
#           "timestamp": sessionIdEndTime,
#           "totalFgTime": sessionDuration
#       }
#       pc_session_data.append(session_start)
#       pc_session_data.append(session_end)
#     elif sink == 'Console':
#       session_start = {
#           "openPsid": deviceId,
#           "appSessionId": sessionId,
#           "psnAccountId": accountId,
#           "eventId": "ApplicationSessionStart",
#           "timestamp": sessionIdStartTime,
#           "totalFgTime": 0
#       }
#       session_end = {
#           "openPsid": deviceId,
#           "appSessionId": sessionId,
#           "psnAccountId": accountId,
#           "eventId": "ApplicationSessionEndBi",
#           "timestamp": sessionIdEndTime,
#           "totalFgTime": sessionDuration
#       }
#       console_session_data.append(session_start)
#       console_session_data.append(session_end)

#   pc_df = spark.createDataFrame(pc_session_data)
#   console_df = spark.createDataFrame(console_session_data)

#   pc_df.repartition(1).write.format('delta').mode('append').saveAsTable('gaming_sessionization_demo.rtm_workload.pc_sessions')
#   console_df.repartition(1).write.format('delta').mode('append').saveAsTable('gaming_sessionization_demo.rtm_workload.console_sessions')
#   minute_counter +=1
#   minute_start += timedelta(minutes=1)



# COMMAND ----------

# %sql
# create table gaming_sessionization_demo.rtm_workload.pc_sessions_bkp as select * from gaming_sessionization_demo.rtm_workload.pc_sessions@v59;
# create table gaming_sessionization_demo.rtm_workload.console_sessions_bkp as select * from gaming_sessionization_demo.rtm_workload.console_sessions@v59;

# COMMAND ----------

# Testing Delayed SessionEnd 
# %sql
# insert into gaming_sessionization_demo.rtm_workload.pc_sessions values (9402035216,	'ApplicationSessionStart', '6fb86ebc-3d88-4e31-b913-ea939acacf56', 'd03fae97-999999872866342',	'2025-11-01T00:06:45.000+00:00', 0);
# insert into gaming_sessionization_demo.rtm_workload.pc_sessions values (9402035216,	'ApplicationSessionEndBi', '6fb86ebc-3d88-4e31-b913-ea939acacf56', 'd03fae97-999999872866342',	'2025-11-01T00:10:45.000+00:00', 240);
# select * from gaming_sessionization_demo.rtm_workload.pc_sessions where hostpcid = '6fb86ebc-3d88-4e31-b913-ea939acacf56' order by timestamp

# COMMAND ----------

# Testing another SessionStart while existing session is still active 
# %sql
# insert into gaming_sessionization_demo.rtm_workload.console_sessions values (3432308279, 'ApplicationSessionStart', 'e77de5a1-05fb-46d1-be9c-aff02fc4fede',	'087aa3fe-999999286732815', '2025-11-01T00:08:17.000+00:00',	0);
# insert into gaming_sessionization_demo.rtm_workload.console_sessions values (3432308279, 'ApplicationSessionEndBi',	'e77de5a1-05fb-46d1-be9c-aff02fc4fede',	'087aa3fe-999999286732815',	'2025-11-01T00:13:20.000+00:00', 303)
# select * from gaming_sessionization_demo.rtm_workload.console_sessions_stream where openPsid = 'e77de5a1-05fb-46d1-be9c-aff02fc4fede' order by timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog gaming_sessionization_demo;
# MAGIC create schema if not exists rtm_workload;
# MAGIC use schema rtm_workload;
# MAGIC
# MAGIC drop table if exists gaming_sessionization_demo.rtm_workload.pc_sessions_stream;
# MAGIC drop table if exists gaming_sessionization_demo.rtm_workload.console_sessions_stream;

# COMMAND ----------

from datetime import timedelta

pc_sessions_df = spark.table('gaming_sessionization_demo.rtm_workload.pc_sessions_bkp')
min_max_timestamp = pc_sessions_df.agg(F.min('timestamp').alias("min_timestamp"),F.max('timestamp').alias("max_timestamp")).collect()[0]
start_time = min_max_timestamp.min_timestamp
end_time = min_max_timestamp.max_timestamp

interval = timedelta(seconds=1)

current = start_time
while current < end_time:
    interval_start = current
    interval_end = current + interval - timedelta(microseconds=1)  # inclusive end
    print(f"Interval: {interval_start} — {interval_end}")

    filter_df = pc_sessions_df.filter(F.col('timestamp').between(F.lit(interval_start), F.lit(interval_end)))
    filter_df.repartition(1).write.format('delta').mode('append').saveAsTable('gaming_sessionization_demo.rtm_workload.pc_sessions_stream')

    current += interval


# COMMAND ----------

from datetime import timedelta

console_sessions_df = spark.table('gaming_sessionization_demo.rtm_workload.console_sessions_bkp')
min_max_timestamp = console_sessions_df.agg(F.min('timestamp').alias("min_timestamp"),F.max('timestamp').alias("max_timestamp")).collect()[0]
start_time = min_max_timestamp.min_timestamp
end_time = min_max_timestamp.max_timestamp

interval = timedelta(seconds=1)

current = start_time
while current < end_time:
  interval_start = current
  interval_end = current + interval - timedelta(microseconds=1)  # inclusive end
  print(f"Interval: {interval_start} — {interval_end}")

  filter_df = console_sessions_df.filter(F.col('timestamp').between(F.lit(interval_start), F.lit(interval_end)))
  filter_df.repartition(1).write.format('delta').mode('append').saveAsTable('gaming_sessionization_demo.rtm_workload.console_sessions_stream')

  current += interval


# COMMAND ----------

# pc_sessions_df = spark.table('gaming_sessionization_demo.rtm_workload.pc_sessions')
# min_max_timestamp = pc_sessions_df.agg(F.min('timestamp').alias("min_timestamp"),F.max('timestamp').alias("max_timestamp")).collect()[0]
# start_time = min_max_timestamp.min_timestamp
# end_time = min_max_timestamp.max_timestamp

# minute_start = start_time
# while minute_start < end_time:
#   for i in range(12):
#     sec_start = i * 5
#     sec_end = 5 * (i + 1) - 1
#     # Ensure last interval ends at 59
#     if sec_end > 59:
#         sec_end = 59
#     interval_start = minute_start.replace(second=sec_start)
#     interval_end = minute_start.replace(second=sec_end)
#     print(f"Interval: {interval_start} — {interval_end}")
#     filter_df = pc_sessions_df.filter(F.col('timestamp').between(F.lit(interval_start), F.lit(interval_end)))
#     filter_df.repartition(1).write.format('delta').mode('append').saveAsTable('gaming_sessionization_demo.rtm_workload.pc_sessions_stream')

#   minute_start += timedelta(minutes=1)

# COMMAND ----------

# console_sessions_df = spark.table('gaming_sessionization_demo.rtm_workload.console_sessions')
# min_max_timestamp = console_sessions_df.agg(F.min('timestamp').alias("min_timestamp"),F.max('timestamp').alias("max_timestamp")).collect()[0]
# start_time = min_max_timestamp.min_timestamp
# end_time = min_max_timestamp.max_timestamp

# minute_start = start_time
# while minute_start < end_time:
#   for i in range(12):
#     sec_start = i * 5
#     sec_end = 5 * (i + 1) - 1
#     # Ensure last interval ends at 59
#     if sec_end > 59:
#         sec_end = 59
#     interval_start = minute_start.replace(second=sec_start)
#     interval_end = minute_start.replace(second=sec_end)
#     print(f"Interval: {interval_start} — {interval_end}")
#     filter_df = console_sessions_df.filter(F.col('timestamp').between(F.lit(interval_start), F.lit(interval_end)))
#     filter_df.repartition(1).write.format('delta').mode('append').saveAsTable('gaming_sessionization_demo.rtm_workload.console_sessions_stream')

#   minute_start += timedelta(minutes=1)

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE  gaming_sessionization_demo.rtm_workload.pc_sessions_stream DISABLE PREDICTIVE OPTIMIZATION;
# MAGIC ALTER TABLE  gaming_sessionization_demo.rtm_workload.console_sessions_stream DISABLE PREDICTIVE OPTIMIZATION;