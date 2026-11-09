# APT-enabled Spark app image for EKS (Spark 4.3.0 GA).
#
# Async Progress Tracking (SPARK-58131) ships INSIDE Spark 4.3.0, so there is NO source build
# and NO jar-swap: we just start FROM the official 4.3.0 release image and bake in the compiled
# RTM pipeline jars + the Kafka connector + S3A jars. APT itself is turned on by a *config flag*
# in the SparkApplication manifest (see eks/jobs/p6/spark-rtm-pr.yaml), not by this image.
#
# Build context = repo root. Produces an amd64 image for m5 nodes.
FROM apache/spark:4.3.0-scala2.13-java17-ubuntu

USER root
ARG SCALA_BIN=2.13
ARG SPARK_VER=4.3.0

# Python for the measured PySpark RTM job (executors run no Python — out_ts via reflect()).
RUN apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3 \
 && ln -sf /usr/bin/python3 /usr/local/bin/python3 && rm -rf /var/lib/apt/lists/*

# The compiled RTM pipeline jars (Scala + Java) and the measured PySpark job.
COPY spark/scala-rtm/target/scala-${SCALA_BIN}/scala-rtm_${SCALA_BIN}-0.1.0.jar /opt/spark/jars/scala-rtm.jar
COPY spark/java-rtm/target/java-rtm.jar /opt/spark/jars/java-rtm.jar
COPY spark/pyspark-bridge/rtm_pyspark_measured.py /opt/spark/work-dir/rtm_pyspark_measured.py

# Kafka structured-streaming connector + transitive deps, plus hadoop-aws + AWS SDK bundle for
# S3A durable checkpoints. These are the exact versions Spark 4.3.0 ships with, verified against
# the 4.3.0 release POMs: kafka.version=3.9.2, commons-pool2.version=2.13.1, hadoop.version=3.5.0,
# aws.java.sdk.v2.version=2.35.4 (spark-parent_2.13:4.3.0 / spark-sql-kafka-0-10_2.13:4.3.0).
RUN cd /opt/spark/jars && \
    for url in \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_${SCALA_BIN}/${SPARK_VER}/spark-sql-kafka-0-10_${SCALA_BIN}-${SPARK_VER}.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BIN}/${SPARK_VER}/spark-token-provider-kafka-0-10_${SCALA_BIN}-${SPARK_VER}.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.2/kafka-clients-3.9.2.jar" \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.13.1/commons-pool2-2.13.1.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.5.0/hadoop-aws-3.5.0.jar" \
      "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.35.4/bundle-2.35.4.jar" \
    ; do curl -fsSL -O "$url"; done

RUN chmod -R a+rX /opt/spark/jars
USER spark
