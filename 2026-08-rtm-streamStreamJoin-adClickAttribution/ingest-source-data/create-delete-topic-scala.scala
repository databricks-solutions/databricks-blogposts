// Databricks notebook source
// Databricks notebook source
// Maven dependency: org.apache.kafka:kafka-clients:3.5.1

// COMMAND ----------

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

// COMMAND ----------

val kafkaBootstrapServers = dbutils.secrets.get("<KAFKA_SECRET_SCOPE>", "<KAFKA_BOOTSTRAP_SECRET_KEY>")

val impressionsTopic = "ad_impressions"
val clicksTopic = "ad_clicks"
val outputTopic = "attributed_clicks"

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

deleteTopic(impressionsTopic)
deleteTopic(clicksTopic)
deleteTopic(outputTopic)

// COMMAND ----------

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

// 8 partitions for impressions, 2 for clicks, 8 for output
createTopic(impressionsTopic, 8)
createTopic(clicksTopic, 2)
createTopic(outputTopic, 8)

// COMMAND ----------

adminClient.close()

// COMMAND ----------

