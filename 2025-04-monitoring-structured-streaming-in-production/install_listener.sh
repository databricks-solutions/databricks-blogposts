# ------------------------------------------------------
# Copies the EventHubListener JAR into /databricks/jars.
# This ensures our listener class is loaded in the Spark
# environment when we start a Databricks cluster.
# ------------------------------------------------------
cp /Volumes/org/streaming/artifacts/java/jars/eventhublistener-1.0-SNAPSHOT-jar-with-dependencies.jar /databricks/jars