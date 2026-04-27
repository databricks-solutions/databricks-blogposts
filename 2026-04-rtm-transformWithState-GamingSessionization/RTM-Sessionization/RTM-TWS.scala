// Databricks notebook source
// import org.apache.spark.sql.streaming.{TTLConfig, ValueState, OutputMode, StatefulProcessor, TimeMode, TimerValues}
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.{Dataset, Encoder, Encoders , DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import org.apache.spark.sql.streaming.Trigger
import org.apache.log4j.Logger
import java.sql.Timestamp
import java.time._
import scala.collection.mutable.ListBuffer
import java.time.Duration
import org.apache.spark.sql.execution.streaming.RealTimeTrigger
import org.apache.spark.sql.streaming.{StreamingQueryListener, StreamingQueryProgress}

// COMMAND ----------

dbutils.widgets.dropdown("mode", "RTM", Seq("RTM", "MBM"))
val mode = dbutils.widgets.get("mode")

// COMMAND ----------

spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass", 
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)
// spark.conf.set(
//   "spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled",
//   "true"
// )

if (mode == "RTM") {
  spark.conf.set("spark.sql.shuffle.partitions", "112")  
} else {
  spark.conf.set("spark.sql.shuffle.partitions", "128")
}

// COMMAND ----------

val stream_name = "RTM-tws-poc"
val volume_path = "/Volumes/gaming_sessionization_demo/rtm_workload/write_to_kafka"
val checkpoint_path = s"$volume_path/$stream_name"

dbutils.widgets.text("clean_checkpoint", "yes")
val clean_checkpoint = dbutils.widgets.get("clean_checkpoint")
if (clean_checkpoint == "yes") {
  dbutils.fs.rm(checkpoint_path, true)
}

// COMMAND ----------

import org.json4s._
import org.json4s.jackson.JsonMethods._

class CustomStreamingQueryListener extends StreamingQueryListener {
  implicit val formats: Formats = DefaultFormats
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    println(s"Query started: id=${event.id}, name=${event.name}")
  }
  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress: StreamingQueryProgress = event.progress
    println(s"batchId = ${progress.batchId} " + s"timestamp = ${progress.timestamp} " + s"numInputRows=${progress.numInputRows} " + s"batchDuration=${progress.batchDuration} ")
    val stateOperators = progress.stateOperators(0)
    val stateMetrics = stateOperators.customMetrics
    // print(stateOperators)
    println(
      s"numRowsTotal = ${stateOperators.numRowsTotal} " + s"numRowsUpdated = ${stateOperators.numRowsUpdated} " + 
      s"numExpiredTimers=${stateMetrics.get("numExpiredTimers")} " + s"numRegisteredTimers=${stateMetrics.get("numRegisteredTimers")} " +
      s"rocksdbPutLatency=${stateMetrics.get("rocksdbPutLatency")} " + s"timerProcessingTimeMs=${stateMetrics.get("timerProcessingTimeMs")} "
    )
    if (mode == "RTM") {
      val progress_json = parse(progress.json)
      val latenciesJson: JValue = progress_json \ "latencies"
      println(pretty(render(latenciesJson)))
    }
  }
  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    println(s"Query terminated: id=${event.id}, runId=${event.runId}")
  }
}

val listener = new CustomStreamingQueryListener

// Remove existing instance if present
spark.streams.removeListener(listener)

// Add the listener
spark.streams.addListener(listener)

// COMMAND ----------

val kafka_bootstrap_servers_plaintext = dbutils.secrets.get("gaming-sessionization-rtm-demo", "kafka-bootstrap-servers")
val pc_sessions_topic = "pc_sessions"
val console_sessions_topic = "console_sessions"
val output_topic = "output_sessions"

// COMMAND ----------

val kafka_schema = new StructType()
  .add("appSessionId", LongType)
  .add("eventId", StringType)
  .add("psnAccountId", StringType)
  .add("hostPcId", StringType)         // for pc_sessions, may be null for console_sessions
  .add("openPsid", StringType)         // for console_sessions, may be null for pc_sessions
  .add("timestamp", TimestampType)
  .add("totalFgTime", LongType)

// COMMAND ----------

package org.databricks.TransformWithStateStructs
import java.sql.Timestamp

object SessionStructs {
  case class InputRow(
    topic: String,
    partition: Int,
    offset: Long,
    kafka_timestamp: Timestamp,
    deviceId: String,
    psnAccountId: String,
    appSessionId: Long,
    eventId: String,
    session_timestamp: Timestamp,
    totalFgTime: Long,
  )

  case class OutputRow(
    deviceId: String,
    appSessionId: Long,  
    psnAccountId: String,
    sessionStatus: String,
    session_timestamp: Timestamp,
    sessionDuration: Long,
    upstream_timestamp: Timestamp,
    processing_timestamp: Timestamp,
    timer_info: Timestamp,
    debug_info: String
  )

  case class MeasuredSessionValue(
    psnAccountId: String,
    sessionStatus: String,
    session_timestamp: Timestamp,
    sessionDuration: Long,
    upstream_timestamp: Timestamp,
    processing_timestamp: Timestamp,
    timer_info: Timestamp,
  )
}

// COMMAND ----------

implicit class PipeOps[A](val a: A) extends AnyVal {
  def pipeIf(cond: Boolean)(f: A => A): A = if (cond) f(a) else a
}

val input_stream_df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
  .option("subscribe", s"$pc_sessions_topic,$console_sessions_topic")
  .option("startingOffsets", "earliest")
  .pipeIf(mode == "RTM")(_.option("maxPartitions", 16))
  .load()
  .withColumn("key", col("key").cast("string"))
  .withColumn("value", col("value").cast("string"))
  .withColumnRenamed("timestamp", "kafka_timestamp")
  .withColumn("session", from_json(col("value"), kafka_schema))
  .withColumn(
    "deviceId",
    when(col("topic") === pc_sessions_topic, col("session.hostPcId"))
      .when(col("topic") === console_sessions_topic, col("session.openPsid"))
      .otherwise(lit(null))
  )
  .selectExpr(
    "topic", "partition", "offset", "kafka_timestamp", 
    "deviceId", "session.psnAccountId", "session.appSessionId","session.eventId","session.timestamp as session_timestamp", "session.totalFgTime"
  )

// COMMAND ----------

// DBTITLE 1,Untitled
// import SessionStructs._
import org.databricks.TransformWithStateStructs.SessionStructs._

class Sessionization() extends StatefulProcessor[String, InputRow, OutputRow] {

  @transient protected var sessionStatusState: MapState[Long, MeasuredSessionValue] = _

  val TIMER_THRESHOLD_IN_MS = 30000
  val SESSION_THRESHOLD_IN_SECONDS = 1800
  val DEBUG_INFO_1 = "SessionStart" 
  val DEBUG_INFO_2 = "SessionEnd for existing Session"
  val DEBUG_INFO_3 = "SessionEnd due to THRESHOLD" 
  val DEBUG_INFO_4 = "SessionEnd due to another SessionStart"

  override def init(
      outputMode: OutputMode,
      timeMode: TimeMode): Unit = {

    sessionStatusState = getHandle.getMapState[Long, MeasuredSessionValue]("sessionStatusState", Encoders.scalaLong, Encoders.product[MeasuredSessionValue], TTLConfig.NONE)
  }

  override def handleInputRows(
    key: String,
    inputRows: Iterator[InputRow],
    timerValues: TimerValues): Iterator[OutputRow] = {

    var outputRows = ListBuffer[OutputRow]()

    inputRows.foreach { row =>
      // Check if the incoming record is for session start
      if (row.eventId == "ApplicationSessionStart") {
        // Check if there is already an active Session for the Device 
        if(sessionStatusState.exists()) {
          // pull the existing sessionId from statestore, generate sessionEnd record, and emit that
          val sessionKeyIter = sessionStatusState.keys()
          for (sessionId <- sessionKeyIter) {
            val outputRecord = generateSessionEnd(key, null, sessionId, timerValues)
            outputRows.append(outputRecord) 
          }     
        }
        // Generate sessionStart record, and emit that
        val outputRecord = generateSessionStart(key, row, timerValues)
        outputRows.append(outputRecord)
      } // checking if the incoming record is for session end
      else if (row.eventId == "ApplicationSessionEndBi") {
        // There should already an active Session for the Device 
        if(sessionStatusState.exists()) {
          // Generate sessionEnd record, and emit that
          val outputRecord = generateSessionEnd(key, row, -1L, timerValues)
          outputRows.append(outputRecord)
        } else {
          // This can happen only of the sessionEnd event comes after the session has already been removed due to SESSION_THRESHOLD_IN_SECONDS.
        }
      }
    }
    
    outputRows.iterator
  }

  def generateSessionStart(key: String, inputRow: InputRow, timerValues: TimerValues): OutputRow = {
    
    // Get the processing time of the current micro-batch, remains same for every record in the same micro-batch
    val currentProcessingTimeMillis = timerValues.getCurrentProcessingTimeInMs()
    val currentProcessingTime = new Timestamp(currentProcessingTimeMillis)

    // create a new timer for next 30 sec, current processing time + 30 sec  
    val timerMillis = currentProcessingTimeMillis + TIMER_THRESHOLD_IN_MS
    val timerTime = new Timestamp(timerMillis)

    // Generate sessionStart record
    val outputRecord = OutputRow(
      key, 
      inputRow.appSessionId, 
      inputRow.psnAccountId, 
      "SessionStart",
      inputRow.session_timestamp,
      0,
      inputRow.kafka_timestamp,
      currentProcessingTime,
      timerTime,
      DEBUG_INFO_1
    )

    // Create a key-value entry into the map state 
    val mapKey = inputRow.appSessionId
    val mapValue = MeasuredSessionValue(
      inputRow.psnAccountId,
      "SessionStart",
      inputRow.session_timestamp,
      0,
      inputRow.kafka_timestamp,
      currentProcessingTime,
      timerTime
    )
    sessionStatusState.updateValue(mapKey, mapValue)
    
    // Set the timer for current batch processingTime + 30 seconds 
    getHandle.registerTimer(timerMillis)

    outputRecord
  }  

  def generateSessionEnd(key: String, inputRow: InputRow, sessionId: Long, timerValues: TimerValues): OutputRow = {
    // Get the processing time of the current micro-batch, remains same for every record in the same micro-batch     
    val currentProcessingTimeMillis = timerValues.getCurrentProcessingTimeInMs()
    val currentProcessingTime = new Timestamp(currentProcessingTimeMillis)
    
    // Generate a sessionEnd record, and emit that
    var outputRecord:OutputRow = null
    if (inputRow != null) {
      outputRecord = OutputRow(
        key, 
        inputRow.appSessionId, 
        inputRow.psnAccountId, 
        "SessionEnd",
        inputRow.session_timestamp,
        inputRow.totalFgTime,
        inputRow.kafka_timestamp,
        currentProcessingTime,
        null,
        DEBUG_INFO_2
      )
      // remove the session from state
      sessionStatusState.removeKey(inputRow.appSessionId)
    } else {
        val sessionValue = sessionStatusState.getValue(sessionId)
        val currentSessionDuration: Long = (currentProcessingTimeMillis - sessionValue.processing_timestamp.getTime)/1000
        outputRecord = OutputRow(
          key, 
          sessionId, 
          sessionValue.psnAccountId, 
          "SessionEnd",
          sessionValue.session_timestamp,
          currentSessionDuration,
          sessionValue.upstream_timestamp,
          currentProcessingTime,
          null,
          DEBUG_INFO_4
        )
        // remove the session from state
        sessionStatusState.removeKey(sessionId)
    }

    // Delete all the timers for the deviceId  
    val timerIter = getHandle.listTimers()
    for (timer <- timerIter) getHandle.deleteTimer(timer)    
    
    outputRecord
  }  

  // Define the logic for handling expired timers
  override def handleExpiredTimer(
    key: String,
    timerValues: TimerValues,
    expiredTimerInfo: ExpiredTimerInfo): Iterator[OutputRow] = {
    
    // Get the processing time of the current micro-batch, remains same for every record in the same micro-batch
    val currentProcessingTimeMillis = timerValues.getCurrentProcessingTimeInMs()
    val currentProcessingTime = new Timestamp(currentProcessingTimeMillis)
    
    // Get the expired Timer info 
    val expiredTimerMillis = expiredTimerInfo.getExpiryTimeInMs()
    val expiredTimerTime = new Timestamp(expiredTimerMillis)
   
    // create a new timer for next 30 sec, current timer expiry time + 30 sec  
    val nextTimerMillis = expiredTimerMillis + TIMER_THRESHOLD_IN_MS
    val nextTimerTime = new Timestamp(nextTimerMillis)

    var outputRows = ListBuffer[OutputRow]()

    // Expecting an entry for the expired timer for a given sessionId. 
    if (sessionStatusState.exists()) {
      
      // pull the existing sessionId, generate sessionHeartbeat record, and emit that
      val sessionIdKeyIter = sessionStatusState.keys()
      
      for (sessionId <- sessionIdKeyIter) {
        var sessionValue = sessionStatusState.getValue(sessionId)
        
        // Caluate the sessionDuration for heartbeat records, Current processing time - SessionStart event processing time
        val currentSessionDuration: Long = (currentProcessingTimeMillis - sessionValue.processing_timestamp.getTime)/1000
        val sessionEvent = "SessionHeartbeat"
        var outputRecord:OutputRow = null

        if (currentSessionDuration < SESSION_THRESHOLD_IN_SECONDS) {
          // Generate sessionHeartbeat record, and emit that
          outputRecord = OutputRow(
            key, 
            sessionId, 
            sessionValue.psnAccountId, 
            sessionEvent,
            sessionValue.session_timestamp,
            currentSessionDuration,
            sessionValue.upstream_timestamp,
            currentProcessingTime,
            nextTimerTime,
            "Timer expired at: " + expiredTimerTime
          )
          // create a new timer for next 30 sec, current processing time + 30 sec
          getHandle.registerTimer(nextTimerMillis)
        } else {
            outputRecord = OutputRow(
              key, 
              sessionId, 
              sessionValue.psnAccountId, 
              "SessionEnd",
              sessionValue.session_timestamp,
              currentSessionDuration,
              sessionValue.upstream_timestamp,
              currentProcessingTime,
              null,
              DEBUG_INFO_3
            )
            // remove the session from state
            sessionStatusState.removeKey(sessionId)            
        }
        outputRows.append(outputRecord)
      }
    }
    outputRows.iterator
  }

}

// COMMAND ----------

val processed_stream_df = input_stream_df.as[InputRow]
  .groupByKey(_.deviceId)
  .transformWithState(
    new Sessionization(), 
    TimeMode.ProcessingTime,
    OutputMode.Update()
  )
  .toDF()
  .withColumn("value", struct(
    col("deviceId"),
    col("appSessionId"),
    col("psnAccountId"),
    col("sessionStatus"),
    col("session_timestamp"),
    col("sessionDuration"),
    col("upstream_timestamp"),
    col("processing_timestamp"),
    col("timer_info"),
    col("debug_info")
  ))
  .select(to_json(col("value")).alias("value"))

// COMMAND ----------

processed_stream_df
  .writeStream
  .queryName("sessionization")
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)  
  .option("topic", output_topic) 
  .option("checkpointLocation", checkpoint_path)
  .trigger(
    if (mode == "RTM") RealTimeTrigger.apply("5 minutes")
    else Trigger.ProcessingTime("0.5 seconds")
  )
  .outputMode("update")
  .start()

// COMMAND ----------



// COMMAND ----------

