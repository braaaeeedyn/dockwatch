# Spark 3.5 (Java 17) with the Iceberg, AWS and Kafka jars baked in, so jobs start offline and fast.
FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root
ARG ICEBERG=1.6.1
ARG SPARK=3.5.5
ARG M2=https://repo1.maven.org/maven2
RUN cd /opt/spark/jars && for jar in \
      org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG}/iceberg-spark-runtime-3.5_2.12-${ICEBERG}.jar \
      org/apache/iceberg/iceberg-aws-bundle/${ICEBERG}/iceberg-aws-bundle-${ICEBERG}.jar \
      org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK}/spark-sql-kafka-0-10_2.12-${SPARK}.jar \
      org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK}/spark-token-provider-kafka-0-10_2.12-${SPARK}.jar \
      org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
      org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    ; do wget -q "${M2}/${jar}" || exit 1; done

# pandas + pyarrow for applyInPandasWithState; pydantic-settings for dockwatch.config; pytest for tests/spark
RUN pip install --no-cache-dir "pandas==2.2.3" "pyarrow==17.0.0" "pydantic==2.9.2" "pydantic-settings==2.5.2" "pytest==8.3.3"

ENV PYTHONPATH=/opt/dockwatch/src
WORKDIR /opt/dockwatch
USER spark
