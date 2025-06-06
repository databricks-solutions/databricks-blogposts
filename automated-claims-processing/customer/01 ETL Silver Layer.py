# Databricks notebook source
# MAGIC %md
# MAGIC # 🔄 Notebook: 01 ETL Silver Layer
# MAGIC
# MAGIC This notebook handles the **Silver Layer (Data Transformation & Processing)** in the Medallion Architecture of the AI-powered claims pipeline. It focuses on **audio conversion**, **metadata extraction**, and **speech-to-text transcription** using the [OpenAI Whisper model](https://openai.com/index/whisper/).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 🧱 Purpose
# MAGIC
# MAGIC To convert raw audio files into a consistent format (MP3), calculate metadata (duration), and transcribe the content into structured text to support downstream AI analytics.
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Install required libraries
# MAGIC %pip install pydub mutagen openai-whisper numpy>=1.24
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Load Initial Resources for Notebook Execution
# MAGIC %run "./resources/init" 

# COMMAND ----------

# DBTITLE 1,Load Unprocessed Audio Metadata
bronze_df = spark.table(f"{CATALOG}.{SCHEMA}.{BRONZE_TABLE}")

meta_table_name = f"{CATALOG}.{SCHEMA}.{META_TABLE}"
if spark._jsparkSession.catalog().tableExists(meta_table_name):
    processed_df = spark.table(meta_table_name).filter("processed = True")
    file_reference_df = bronze_df.join(processed_df, "file_name", "left_anti")
else:
    file_reference_df = bronze_df

if file_reference_df.isEmpty():
    dbutils.notebook.exit("✅ No new files to process. Exiting Silver Layer.")

# COMMAND ----------

# DBTITLE 1,Convert Raw Audio → MP3
from pydub import AudioSegment
import os

dbutils.fs.mkdirs(mp3_audio_path)

# Convert each file to mp3 and save to the new volume
for row in file_reference_df.collect():
    file_path = row['file_path']
    try:
        audio = AudioSegment.from_file(file_path)
        new_file_path = os.path.join(mp3_audio_path, os.path.basename(file_path).replace(os.path.splitext(file_path)[1], ".mp3"))
        audio.export(new_file_path, format="mp3")
    except Exception as e:
        print(f"⚠️ Error converting {file_path}: {e}")

# COMMAND ----------

# DBTITLE 1,Extract Audio Durations (MP3)
from mutagen.mp3 import MP3
from pyspark.sql.types import FloatType
import pyspark.sql.functions as F

mp3_df = (
    spark.createDataFrame(dbutils.fs.ls(mp3_audio_path))
    .withColumn("file_path", F.expr("substring(path, 6, length(path))"))
    .withColumn("file_name", F.expr("substring(name, 1, length(name) - 4)"))
    .filter(F.col("file_name").isin([r["file_name"] for r in file_reference_df.collect()]))
)

def get_audio_duration(file_path):
    try:
        audio = MP3(file_path)
        return float(audio.info.length)
    except Exception as e:
        print(f"⚠️ Error getting duration for {file_path}: {e}")
        return None

duration_udf = F.udf(get_audio_duration, FloatType())
mp3_df = mp3_df.withColumn("audio_duration", F.round(duration_udf("file_path"), 0))

display(mp3_df)

# COMMAND ----------

# DBTITLE 1,Transcribe Audio with Whisper
import whisper

model = whisper.load_model("small")
print("✅ Whisper model loaded.")

def transcribe_audio(file_path: str, model: whisper.Whisper) -> str:
    try:
        result = model.transcribe(file_path)
        return result["text"]
    except Exception as e:
        print(f"⚠️ Transcription failed for {file_path}: {e}")
        return ""
      
from pyspark.sql.types import StringType
# from pyspark.sql.functions import udf

# transcribe_udf = udf(lambda path: transcribe_audio(path), StringType())

# transcribed_df = mp3_df.withColumn("transcription", transcribe_udf("file_path"))

# Collect the file paths to the driver
file_paths = mp3_df.select("file_path").rdd.flatMap(lambda x: x).collect()

# Transcribe the audio files outside of Spark
transcriptions = [transcribe_audio(file_path, model) for file_path in file_paths]

# Create a DataFrame with the transcriptions
transcriptions_df = spark.createDataFrame(zip(file_paths, transcriptions), ["file_path", "transcription"])

# Join the transcriptions back to the original DataFrame
transcribed_df = mp3_df.join(transcriptions_df, on="file_path", how="inner") \
                                 .select("file_path", "file_name", "transcription", "audio_duration")


# COMMAND ----------

# DBTITLE 1,Enrich with Metadata
from pyspark.sql.functions import col, split, regexp_replace, to_timestamp, concat_ws

transcribed_df = transcribed_df \
    .withColumn("file_name", regexp_replace(col("file_name"), ".mp3", "")) \
    .withColumn("call_id", split(col("file_name"), "_").getItem(0)) \
    .withColumn("agent_id", split(col("file_name"), "_").getItem(1)) \
    .withColumn("call_datetime", 
        to_timestamp(
            concat_ws(":", split(col("file_name"), "_").getItem(2), 
            split(col("file_name"), "_").getItem(3), 
            split(col("file_name"), "_").getItem(4)))
    )

display(transcribed_df.select("file_name", "call_id", "agent_id", "call_datetime", "audio_duration", "transcription"))

# COMMAND ----------

# DBTITLE 1,Write to Silver Table
silver_table_path = f"{CATALOG}.{SCHEMA}.{SILVER_TABLE}"

if not spark._jsparkSession.catalog().tableExists(silver_table_path):
    transcribed_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(silver_table_path)
else:
    transcribed_df.write.mode("append").saveAsTable(silver_table_path)

print(f"✅ Transcriptions written to Silver table: {silver_table_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Output
# MAGIC - A clean, enriched Delta table: transcriptions_silver
# MAGIC - Includes transcription text, call metadata, and audio duration for each entry.