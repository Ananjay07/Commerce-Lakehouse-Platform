FROM apache/airflow:2.7.2-python3.10

USER root
# Install OpenJDK 17 and other utilities
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         procps \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# Install python packages needed for Spark, Kafka, Great Expectations and OpenLineage
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    great_expectations==0.18.12 \
    confluent-kafka==2.3.0 \
    boto3==1.34.34 \
    requests==2.31.0 \
    pandas==2.1.4 \
    openlineage-python \
    apache-airflow-providers-openlineage

# Pre-download and cache Spark Maven packages in the image to avoid runtime download timeouts
RUN python -c "from pyspark.sql import SparkSession; SparkSession.builder.config('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,io.openlineage:openlineage-spark_2.12:1.9.1').getOrCreate().stop()"
