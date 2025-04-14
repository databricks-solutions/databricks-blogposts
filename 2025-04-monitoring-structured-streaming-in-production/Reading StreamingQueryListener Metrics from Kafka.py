# Databricks notebook source
# Set-up options for reading from Event Hubs with kafka using OAUTH:
eh_server = 'streaming_query_metrics.servicebus.windows.net'
client_id = dbutils.secrets.get('secret-scope', 'client-id')
client_secret = dbutils.secrets.get('secret-scope', 'client-secret')
tenant_id = dbutils.secrets.get('secret-scope', 'tenant-id')

sasl_config = f'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule' + \
     f' required clientId="{client_id}" clientSecret="{client_secret}"' + \
     f' scope="https://{eh_server}/.default" ssl.protocol="SSL";'
oauth_endpoint = f'https://login.microsoft.com/{tenant_id}/oauth2/v2.0/token'

options = {
  'kafka.bootstrap.servers': f'{eh_server}:9093',
  'kafka.security.protocol': 'SASL_SSL',
  'kafka.sasl.mechanism': 'OAUTHBEARER',
  'kafka.sasl.jaas.config': sasl_config,
  'kafka.sasl.oauthbearer.token.endpoint.url': oauth_endpoint,
  'kafka.sasl.login.callback.handler.class': 'kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler',
  'kafka.request.timeout.ms': '60000',
  'kafka.session.timeout.ms': '30000',
  'subscribe': 'event-hub-001'
}

# COMMAND ----------

# Read listener metrics that were forwarded to Event Hubs:
df = spark.readStream \
  .format('kafka') \
  .options(**options) \
  .load()

# COMMAND ----------

# Write the listener metrics to a Delta table in Unity Catalog:
df.writeStream \
  .trigger(processingTime='10 seconds') \
  .queryName('eventhub_read_001') \
  .format('delta') \
  .option('checkpointLocation', '/Volumes/org/streaming/checkpoints/listener_metrics_checkpoint_001') \
  .table('org.streaming.listener_metrics')