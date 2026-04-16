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

spark.conf.set(
  "spark.sql.streaming.stateStore.providerClass", 
  "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
)
// spark.conf.set(
//   "spark.databricks.streaming.statefulOperator.asyncCheckpoint.enabled",
//   "true"
// )
spark.conf.set("spark.sql.shuffle.partitions", "64")

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
    val progress_json = parse(progress.json)
    val latenciesJson: JValue = progress_json \ "latencies"
    println(pretty(render(latenciesJson)))
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

val input_stream_df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext)
  .option("subscribe", s"$pc_sessions_topic,$console_sessions_topic")
  .option("startingOffsets", "earliest")
  .option("maxPartitions", 16)
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
  val DEBUG_INFO_1 = "No Existing DeviceID-SessionStart"
  val DEBUG_INFO_2 = "Existing DeviceID-Another SessionStart"
  val DEBUG_INFO_3 = "Existing DeviceID-SessionEnd for existing Session"
  val DEBUG_INFO_4 = "Existing DeviceID-NoSessionEnd but another SessionStart"

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
        // Generate sessionStart record, and emit that
        val outputRecord = generateSessionStart(key, row, timerValues)
        outputRows.append(outputRecord)
      } // checking if the incoming record is for session end
      else if (row.eventId == "ApplicationSessionEndBi") {
        val outputRecord = generateSessionEnd(key, row, timerValues)
        outputRows.append(outputRecord)
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

  def generateSessionEnd(key: String, inputRow: InputRow, timerValues: TimerValues): OutputRow = {
    // Get the processing time of the current micro-batch, remains same for every record in the same micro-batch     
    val currentProcessingTimeMillis = timerValues.getCurrentProcessingTimeInMs()
    val currentProcessingTime = new Timestamp(currentProcessingTimeMillis)
    
    // Generate a sessionEnd record, and emit that
    var outputRecord = OutputRow(
      key, 
      inputRow.appSessionId, 
      inputRow.psnAccountId, 
      "SessionEnd",
      inputRow.session_timestamp,
      inputRow.totalFgTime,
      inputRow.kafka_timestamp,
      currentProcessingTime,
      null,
      DEBUG_INFO_3
    )

    // remove the session from state
    sessionStatusState.removeKey(inputRow.appSessionId)

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
        
        // Caluate the sessionDuration for heartbeat records, existing duration + 30 seconds 
        val currentSessionDuration: Long = sessionValue.sessionDuration + (currentProcessingTime.getTime - sessionValue.processing_timestamp.getTime)/1000
        val sessionEvent = "SessionHeartbeat"

        // Generate sessionHeartbeat record, and emit that
        val outputRecord = OutputRow(
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
        outputRows.append(outputRecord) 

        // Update the existing key-value entry into the map state 
        val updatedSessionValue = sessionValue.copy(
          sessionStatus = sessionEvent,
          sessionDuration = currentSessionDuration,
          processing_timestamp = currentProcessingTime,
          timer_info = nextTimerTime
        )
        sessionStatusState.updateValue(sessionId, updatedSessionValue)

        // create a new timer for next 30 sec, current processing time + 30 sec
        getHandle.registerTimer(nextTimerMillis)
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
  .select(
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
  )

// COMMAND ----------

import java.sql.{Connection, DriverManager, PreparedStatement, Types}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/**
 * @param username   Database username
 * @param password   Database password
 * @param table      Target table name (e.g., "public.my_table")
 * @param host       Lakebase host address
 * @param mode       Write mode: "insert", "upsert", or "bulk-insert"
 * @param primaryKeys Primary key columns (required for upsert mode)
 * @param batchSize  Number of rows per batch (default: 1000)
 * @param batchIntervalMs Max time between flushes in milliseconds (default: 100)
 */
case class LakebaseConfig(
    username: String,
    password: String,
    table: String,
    host: String,
    mode: String = "insert",
    primaryKeys: Seq[String] = Seq.empty,
    batchSize: Int = 5000,
    batchIntervalMs: Int = 100,
    queueSize: Int = 50000
) extends Serializable

// ---------------------------------------------------------------------------
// ForeachWriter
// ---------------------------------------------------------------------------

class LakebaseForeachWriter(config: LakebaseConfig, schema: StructType)
    extends ForeachWriter[Row] with Serializable {

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  private val columns: Array[String] = schema.fieldNames
  private val columnTypes: Array[DataType] = schema.fields.map(_.dataType)
  private val sql: Option[String] = buildSql()

  // Validate schema at construction time
  locally {
    val unsupported = findUnsupportedFields(schema)
    if (unsupported.nonEmpty) {
      throw new IllegalArgumentException(
        s"Unsupported field types: ${unsupported.mkString(", ")}. " +
          "Convert complex types to supported formats first."
      )
    }

    val jdbcUrl = s"jdbc:postgresql://${config.host}:5432/databricks_postgres?sslmode=require"
    val checkConn = DriverManager.getConnection(jdbcUrl, config.username, config.password)
    try {
      val columnDefs = schema.fields.map { field =>
        val pgType = sparkTypeToPgType(field.dataType)
        val nullable = if (field.nullable) "" else " NOT NULL"
        s"${field.name} $pgType$nullable"
      }.mkString(", ")

      val pkClause = if (config.primaryKeys.nonEmpty) {
        s", PRIMARY KEY (${config.primaryKeys.mkString(", ")})"
      } else ""

      val createSql =
        s"CREATE TABLE IF NOT EXISTS ${config.table} ($columnDefs$pkClause)"

      val stmt = checkConn.createStatement()
      try {
        stmt.execute(createSql)
      } finally {
        stmt.close()
      }
      logger.info(s"Ensured table ${config.table} exists")
    } finally {
      checkConn.close()
    }
  }

  // Runtime state (initialized in open(), not serialized)
  @transient private var conn: Connection = _
  @transient private var queue: LinkedBlockingQueue[Array[Any]] = _
  @transient private var stopEvent: AtomicBoolean = _
  @transient private var workerThread: Thread = _
  @transient private var batch: ArrayBuffer[Array[Any]] = _
  @transient private var lastFlush: Long = _
  @transient private var workerError: String = _
  @transient private var partitionId: Long = _
  @transient private var epochId: Long = _

  // -------------------------------------------------------------------------
  // ForeachWriter Interface
  // -------------------------------------------------------------------------

  override def open(partitionId: Long, epochId: Long): Boolean = {
    try {
      this.partitionId = partitionId
      this.epochId = epochId

      val jdbcUrl =
        s"jdbc:postgresql://${config.host}:5432/databricks_postgres?sslmode=require"
      conn = DriverManager.getConnection(jdbcUrl, config.username, config.password)
      conn.setAutoCommit(false)

      queue = new LinkedBlockingQueue[Array[Any]](config.queueSize)
      stopEvent = new AtomicBoolean(false)
      batch = ArrayBuffer.empty[Array[Any]]
      lastFlush = System.currentTimeMillis()
      workerError = null

      workerThread = new Thread(() => worker())
      workerThread.setDaemon(true)
      workerThread.start()

      logger.info(s"[$partitionId|$epochId] Opening writer for table ${config.table}")
      true
    } catch {
      case e: Exception =>
        logger.error(s"Failed to open writer: ${e.getMessage}", e)
        false
    }
  }

  override def process(row: Row): Unit = {
    if (workerError != null) {
      throw new RuntimeException(s"Worker failed: $workerError")
    }

    val rowData = new Array[Any](columns.length)
    for (i <- columns.indices) {
      rowData(i) =
        if (row.isNullAt(i)) null
        else convertValue(row.get(i), columnTypes(i))
    }
    queue.put(rowData)
  }

  override def close(error: Throwable): Unit = {
    try {
      if (stopEvent != null) stopEvent.set(true)
      if (workerThread != null && workerThread.isAlive) {
        val maxWaitMs = if (config.mode.toLowerCase == "upsert") 30000 else 5000
        val start = System.currentTimeMillis()
        while (workerThread.isAlive && (System.currentTimeMillis() - start) < maxWaitMs) {
          workerThread.join(1000)
        }
        val waited = System.currentTimeMillis() - start
        if (workerThread.isAlive) {
          logger.warn(s"[$partitionId|$epochId] Worker still alive after ${waited}ms (max: ${maxWaitMs}ms)")
        } else {
          logger.info(s"[$partitionId|$epochId] Worker finished after ${waited}ms")
        }
      }
      if (queue != null && queue.size() > config.batchSize * 5) {
        logger.warn(
          s"[$partitionId|$epochId] Large queue remaining: ${queue.size()}"
        )
      }
      flushRemaining()
    } finally {
      if (conn != null) {
        try conn.close()
        catch { case _: Exception => }
      }
    }
    logger.info(s"[$partitionId|$epochId] Writer closed")
  }

  // -------------------------------------------------------------------------
  // Internal Methods
  // -------------------------------------------------------------------------

  private def worker(): Unit = {
    while (!stopEvent.get()) {
      try {
        var collecting = true
        while (collecting && batch.size < config.batchSize) {
          val item = queue.poll(10, TimeUnit.MILLISECONDS)
          if (item != null) batch += item
          else collecting = false
        }

        if (batch.size >= config.batchSize || (batch.nonEmpty && timeToFlush())) {
          flushBatch()
        }

        Thread.sleep(0, 100000) // 0.1ms
      } catch {
        case e: Exception =>
          logger.error(s"Worker error: ${e.getMessage}", e)
          workerError = e.getMessage
          return
      }
    }
  }

  private def flushBatch(): Unit = {
    if (batch.isEmpty) return

    val perfStart = System.currentTimeMillis()
    try {
      config.mode.toLowerCase match {
        case "bulk-insert" => flushWithCopy()
        case _             => flushWithExecuteBatch()
      }
      conn.commit()

      val flushedCount = batch.size
      batch.clear()
      lastFlush = System.currentTimeMillis()
      val perfTime = System.currentTimeMillis() - perfStart
      logger.info(
        s"[$partitionId|$epochId] Flushed $flushedCount rows in ${perfTime}ms"
      )
    } catch {
      case e: Exception =>
        try conn.rollback()
        catch { case _: Exception => }
        throw e
    }
  }

  private def flushWithCopy(): Unit = {
    val cm = new org.postgresql.copy.CopyManager(
      conn.unwrap(classOf[org.postgresql.core.BaseConnection])
    )
    val cols = columns.mkString(", ")
    val copyIn =
      cm.copyIn(s"COPY ${config.table} ($cols) FROM STDIN WITH (FORMAT text)")

    try {
      for (row <- batch) {
        val line = row.map {
          case null => "\\N"
          case v =>
            v.toString
              .replace("\\", "\\\\")
              .replace("\t", "\\t")
              .replace("\n", "\\n")
              .replace("\r", "\\r")
        }.mkString("\t") + "\n"
        val bytes = line.getBytes("UTF-8")
        copyIn.writeToCopy(bytes, 0, bytes.length)
      }
      copyIn.endCopy()
    } finally {
      if (copyIn.isActive) {
        copyIn.cancelCopy()
      }
    }
  }

  private def flushWithExecuteBatch(): Unit = {
    val stmt = conn.prepareStatement(sql.get)
    try {
      for (row <- batch) {
        for (i <- row.indices) {
          if (row(i) == null) stmt.setNull(i + 1, Types.NULL)
          else setParameter(stmt, i + 1, row(i), columnTypes(i))
        }
        stmt.addBatch()
      }
      stmt.executeBatch()
    } finally {
      stmt.close()
    }
  }

  private def flushRemaining(): Unit = {
    if (queue != null) {
      val remaining = new java.util.ArrayList[Array[Any]]()
      queue.drainTo(remaining)
      remaining.forEach(item => batch += item)
    }
    if (batch != null && batch.nonEmpty) flushBatch()
  }

  private def timeToFlush(): Boolean =
    System.currentTimeMillis() - lastFlush >= config.batchIntervalMs

  // -------------------------------------------------------------------------
  // SQL Builder
  // -------------------------------------------------------------------------

  private def buildSql(): Option[String] = {
    val cols = columns.mkString(", ")
    val placeholders = columns.map(_ => "?").mkString(", ")

    config.mode.toLowerCase match {
      case "insert" =>
        Some(s"INSERT INTO ${config.table} ($cols) VALUES ($placeholders)")

      case "upsert" =>
        if (config.primaryKeys.isEmpty) {
          throw new IllegalArgumentException("primaryKeys required for upsert mode")
        }
        val pkCols = config.primaryKeys.mkString(", ")
        val pkSet = config.primaryKeys.map(_.toLowerCase).toSet
        val updateCols = columns
          .filterNot(c => pkSet.contains(c.toLowerCase))
          .map(c => s"$c = EXCLUDED.$c")
          .mkString(", ")

        if (updateCols.isEmpty) {
          Some(
            s"""INSERT INTO ${config.table} ($cols) VALUES ($placeholders)
               |ON CONFLICT ($pkCols) DO NOTHING""".stripMargin
          )
        } else {
          Some(
            s"""INSERT INTO ${config.table} ($cols) VALUES ($placeholders)
               |ON CONFLICT ($pkCols) DO UPDATE SET $updateCols""".stripMargin
          )
        }

      case "bulk-insert" =>
        None

      case other =>
        throw new IllegalArgumentException(
          s"Invalid mode: $other. Use 'insert', 'upsert', or 'bulk-insert'."
        )
    }
  }

  // -------------------------------------------------------------------------
  // Type Conversion
  // -------------------------------------------------------------------------

  private def convertValue(value: Any, dataType: DataType): Any = {
    if (value == null) return null
    dataType match {
      case BooleanType   => value.asInstanceOf[Boolean]
      case IntegerType   => value match { case i: Int => i; case o => o.toString.toInt }
      case LongType      => value match { case l: Long => l; case o => o.toString.toLong }
      case FloatType     => value match { case f: Float => f; case o => o.toString.toFloat }
      case DoubleType    => value match { case d: Double => d; case o => o.toString.toDouble }
      case ShortType     => value match { case s: Short => s; case o => o.toString.toShort }
      case ByteType      => value match { case b: Byte => b; case o => o.toString.toByte }
      case _: DecimalType =>
        value match {
          case d: java.math.BigDecimal  => d
          case d: scala.math.BigDecimal => d.bigDecimal
          case o                        => new java.math.BigDecimal(o.toString)
        }
      case StringType    => value.toString
      case DateType      => value
      case TimestampType => value
      case BinaryType    => value
      case _             => value
    }
  }

  private def setParameter(
      stmt: PreparedStatement,
      index: Int,
      value: Any,
      dataType: DataType
  ): Unit = {
    dataType match {
      case BooleanType    => stmt.setBoolean(index, value.asInstanceOf[Boolean])
      case IntegerType    => stmt.setInt(index, value.asInstanceOf[Int])
      case LongType       => stmt.setLong(index, value.asInstanceOf[Long])
      case FloatType      => stmt.setFloat(index, value.asInstanceOf[Float])
      case DoubleType     => stmt.setDouble(index, value.asInstanceOf[Double])
      case ShortType      => stmt.setShort(index, value.asInstanceOf[Short])
      case ByteType       => stmt.setByte(index, value.asInstanceOf[Byte])
      case _: DecimalType => stmt.setBigDecimal(index, value.asInstanceOf[java.math.BigDecimal])
      case StringType     => stmt.setString(index, value.asInstanceOf[String])
      case DateType       => stmt.setDate(index, value.asInstanceOf[java.sql.Date])
      case TimestampType  => stmt.setTimestamp(index, value.asInstanceOf[java.sql.Timestamp])
      case BinaryType     => stmt.setBytes(index, value.asInstanceOf[Array[Byte]])
      case _              => stmt.setObject(index, value)
    }
  }

  // -------------------------------------------------------------------------
  // Spark to PostgreSQL Type Mapping
  // -------------------------------------------------------------------------

  private def sparkTypeToPgType(dataType: DataType): String = {
    dataType match {
      case BooleanType        => "BOOLEAN"
      case ByteType           => "SMALLINT"
      case ShortType          => "SMALLINT"
      case IntegerType        => "INTEGER"
      case LongType           => "BIGINT"
      case FloatType          => "REAL"
      case DoubleType         => "DOUBLE PRECISION"
      case d: DecimalType     => s"DECIMAL(${d.precision}, ${d.scale})"
      case StringType         => "TEXT"
      case DateType           => "DATE"
      case TimestampType      => "TIMESTAMP"
      case BinaryType         => "BYTEA"
      case _                  => "TEXT"
    }
  }

  // -------------------------------------------------------------------------
  // Schema Validation
  // -------------------------------------------------------------------------

  private def findUnsupportedFields(schema: StructType): Seq[String] = {
    schema.fields.toSeq.flatMap { field =>
      field.dataType match {
        case _: StructType => Some(field.name)
        case _: MapType    => Some(field.name)
        case arr: ArrayType
            if arr.elementType.isInstanceOf[StructType] ||
              arr.elementType.isInstanceOf[MapType] =>
          Some(field.name)
        case _ => None
      }
    }
  }
}


// COMMAND ----------

val username = dbutils.secrets.get("gaming-sessionization-rtm-demo", "lakebase-jdbc-username")
val password = dbutils.secrets.get("gaming-sessionization-rtm-demo", "lakebase-jdbc-password")
val table = "session_results_rtm_insert" 
// Replace with your Lakebase hostname (never commit a real instance host in public forks).
val host = "YOUR_LAKEBASE_DATABASE_HOST"

val lakebaseConfig = LakebaseConfig(
  username = username,
  password = password,
  table = table,
  host = host,
  mode = "insert", // insert or upsert or bulk-insert
  // primaryKeys = Seq("deviceid", "appsessionid", "psnaccountid", "sessionstatus")
)

processed_stream_df
  .writeStream
  .queryName("sessionization")
  .option("checkpointLocation", checkpoint_path)
  .outputMode("update")
  .trigger(RealTimeTrigger.apply("5 minutes"))
  .foreach(new LakebaseForeachWriter(lakebaseConfig, processed_stream_df.schema))
  .start()

// COMMAND ----------



// COMMAND ----------

