FROM apache/airflow:2.8.0

USER root

# Install Spark and Java dependencies
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment (Java 17 is default in Debian Bookworm)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$PATH:$JAVA_HOME/bin

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV SPARK_URL="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"
ENV SPARK_MIRROR_URL="https://mirrors.huaweicloud.com/apache/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"

# Download Spark with retry logic - try Apache archive first, fallback to Huawei mirror
RUN cd /tmp && \
    (curl -fL "${SPARK_URL}" -o spark.tgz || curl -fL "${SPARK_MIRROR_URL}" -o spark.tgz) && \
    tar -xz -f spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    chown -R airflow:root ${SPARK_HOME} && \
    rm -f spark.tgz

ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYTHONPATH=$PYTHONPATH:${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install --no-cache-dir apache-airflow-providers-apache-spark

# Copy Spark job code
COPY --chown=airflow:root src /opt/airflow/dags/src

# Copy connection initialization script
COPY --chown=airflow:root airflow/scripts/init-connections.sh /init-connections.sh
RUN chmod +x /init-connections.sh

# Set working directory
WORKDIR /opt/airflow

