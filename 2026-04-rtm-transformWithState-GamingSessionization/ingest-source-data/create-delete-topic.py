# Databricks notebook source
# MAGIC %pip install --index-url https://pypi-proxy.dev.databricks.com/simple  kafka-python 

# COMMAND ----------


from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# COMMAND ----------

kafka_bootstrap_servers_plaintext = dbutils.secrets.get("gaming-sessionization-rtm-demo", "kafka-bootstrap-servers")

# Kafka topics
pc_sessions_topic = 'pc_sessions'
console_sessions_topic = 'console_sessions'
output_topic = "output_sessions"

# COMMAND ----------

def delete_topic(topic_name):
    # Configure Admin client
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_bootstrap_servers_plaintext.split(','),
        client_id="delete-topic-client"
    )

    # Delete the topic
    try:
        admin_client.delete_topics(topics=[topic_name])
        print(f"Topic '{topic_name}' marked for deletion.")
    except Exception as e:
        print(f"Failed to delete topic '{topic_name}': {e}")

delete_topic(pc_sessions_topic)
delete_topic(console_sessions_topic)
delete_topic(output_topic)

# COMMAND ----------

# retention_ms = 7 * 24 * 60 * 60 * 1000  # 7 days in milliseconds
retention_ms = -1

def create_topic(topic_name, num_partitions):

    # Configure Kafka admin client
    create_client = KafkaAdminClient(
        bootstrap_servers=kafka_bootstrap_servers_plaintext.split(','),  # Replace with your Kafka broker(s)
        client_id="create-topic-client"
    )

    # Define a new topic
    topic = NewTopic(
        name=topic_name,
        num_partitions=num_partitions,
        replication_factor=3,
        topic_configs={
            'retention.ms': str(retention_ms)
        }    
    )

    # Create the topic
    try:
        create_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully.")
    except Exception as e:
        print(f"Error creating topic: {e}")
    finally:
        create_client.close()

create_topic(pc_sessions_topic, 16)
create_topic(console_sessions_topic, 16)
create_topic(output_topic, 64)

# COMMAND ----------



# COMMAND ----------

