// Databricks notebook source
// Maven dependency: org.apache.kafka:kafka-clients:3.5.1

// COMMAND ----------

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

// COMMAND ----------

val kafkaBootstrapServers = dbutils.secrets.get("gaming-sessionization-rtm-demo", "kafka-bootstrap-servers")

val pcSessionsTopic = "pc_sessions"
val consoleSessionsTopic = "console_sessions"
val outputTopic = "output_sessions"

// COMMAND ----------

val props = new Properties()
props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
val adminClient = AdminClient.create(props)

// COMMAND ----------

def deleteTopic(topicName: String): Unit = {
  try {
    adminClient.deleteTopics(Collections.singletonList(topicName)).all().get()
    println(s"Topic '$topicName' deleted.")
  } catch {
    case e: Exception => println(s"Failed to delete topic '$topicName': ${e.getMessage}")
  }
}

deleteTopic(pcSessionsTopic)
deleteTopic(consoleSessionsTopic)
deleteTopic(outputTopic)

// COMMAND ----------

// val retentionMs = 7 * 24 * 60 * 60 * 1000  // 7 days
val retentionMs = -1

def createTopic(topicName: String, numPartitions: Int, replicationFactor: Short = 3): Unit = {
  val topic = new NewTopic(topicName, numPartitions, replicationFactor)
    .configs(Map("retention.ms" -> retentionMs.toString).asJava)
  try {
    adminClient.createTopics(Collections.singletonList(topic)).all().get()
    println(s"Topic '$topicName' created successfully.")
  } catch {
    case e: Exception => println(s"Error creating topic '$topicName': ${e.getMessage}")
  }
}

createTopic(pcSessionsTopic, 16)
createTopic(consoleSessionsTopic, 16)
createTopic(outputTopic, 64)

// COMMAND ----------

adminClient.close()

// COMMAND ----------

