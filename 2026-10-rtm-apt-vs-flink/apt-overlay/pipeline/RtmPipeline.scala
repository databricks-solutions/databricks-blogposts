import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

/**
 * Stateless Kafka -> transform -> Kafka pipeline for the Spark RTM vs Flink benchmark.
 *
 * Two modes, selected by arg, running the SAME transform:
 *   rtm        : Trigger.RealTime(<duration>), outputMode("update")  -- the article's claim
 *   microbatch : default processing-time trigger, outputMode("append") -- the latency floor
 *
 * Args (positional):
 *   0 mode            "rtm" | "microbatch"
 *   1 bootstrap       e.g. kafka:9092
 *   2 inputTopic      e.g. input-events
 *   3 outputTopic     e.g. output-events
 *   4 checkpointDir   e.g. /tmp/spark-checkpoints/rtm
 *   5 triggerDuration e.g. "5 seconds"  (rtm only; checkpoint cadence, NOT a latency target)
 */
object RtmPipeline {

  // Input event contract (see common/event_schema.md).
  val eventSchema: StructType = StructType(Seq(
    StructField("event_id",   LongType),
    StructField("user_id",    LongType),
    StructField("event_type", StringType),
    StructField("country",    StringType),
    StructField("amount",     DoubleType),
    StructField("ts",         DoubleType)   // epoch seconds (float), latency anchor
  ))

  def main(args: Array[String]): Unit = {
    val mode            = args(0)
    val bootstrap       = args(1)
    val inputTopic      = args(2)
    val outputTopic     = args(3)
    val checkpointDir   = args(4)
    val triggerDuration = if (args.length > 5) args(5) else "5 seconds"

    // Per-ROW wall-clock stamp. current_timestamp() is frozen at epoch/batch start, which
    // in RTM's long-running tasks is stamped when the task launched (BEFORE most rows are
    // written) -> negative/garbage pipeline latency. A non-deterministic UDF is evaluated
    // per row at actual processing time, giving a true out_ts. (Documented RTM gotcha.)
    val nowEpoch = udf(() => System.currentTimeMillis() / 1000.0).asNondeterministic()

    val spark = SparkSession.builder()
      .appName(s"rtm-bench-$mode")
      // RTM does not support Adaptive Query Execution.
      .config("spark.sql.adaptive.enabled", "false")
      // Shuffle partitions irrelevant (stateless, no shuffle) but keep it lean.
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val reader = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "latest")
    // maxOffsetsPerTrigger is incompatible with RTM; only bound batch size in micro-batch.
    val raw = (if (mode == "microbatch") reader.option("maxOffsetsPerTrigger", "200000")
               else reader).load()

    val parsed = raw
      .selectExpr("CAST(value AS STRING) AS json_str")
      .select(from_json(col("json_str"), eventSchema).alias("e"))
      .select("e.*")

    // Identical stateless transform across engines.
    val transformed = parsed
      .filter(col("event_type").isin("purchase", "add_to_cart") && (col("amount") > 0))
      .withColumn("amount_with_tax", round(col("amount") * lit(1.21), 2))
      .withColumn("country", upper(col("country")))
      // out_ts = engine processing-time at write, epoch seconds (float) to match ts.
      .withColumn("out_ts", nowEpoch())

    // Serialize to the output contract; key by user_id.
    val out = transformed.select(
      col("user_id").cast(StringType).alias("key"),
      to_json(struct(
        col("event_id"), col("user_id"), col("event_type"), col("country"),
        col("amount"), col("amount_with_tax"), col("ts"), col("out_ts")
      )).alias("value")
    )

    val baseWriter = out.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrap)
      .option("topic", outputTopic)
      .option("checkpointLocation", checkpointDir)

    val query = mode match {
      case "rtm" =>
        baseWriter
          .outputMode("update")
          .trigger(Trigger.RealTime(triggerDuration))
          .start()
      case "microbatch" =>
        baseWriter
          .outputMode("append")           // default processing-time trigger = latency floor
          .start()
      case other =>
        throw new IllegalArgumentException(s"unknown mode '$other' (use rtm|microbatch)")
    }

    println(s"[RtmPipeline] mode=$mode id=${query.id} runId=${query.runId} " +
      s"trigger=${if (mode == "rtm") s"RealTime($triggerDuration)" else "micro-batch(default)"}")
    query.awaitTermination()
  }
}
