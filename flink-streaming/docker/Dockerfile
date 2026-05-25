# PyFlink job image: Flink 1.18 + Kafka connector JARs + Python deps
FROM flink:1.18.1-scala_2.12-java11

USER root

ARG FLINK_VERSION=1.18.1
ARG SCALA_VERSION=2.12
ARG KAFKA_CONNECTOR_VERSION=3.2.0-1.18

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Kafka connector for DataStream API (unified sink/source)
RUN curl -fsSL -o /opt/flink/lib/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
        "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
    && curl -fsSL -o /opt/flink/lib/kafka-clients-3.7.0.jar \
        "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar" \
    && curl -fsSL -o /opt/flink/lib/flink-connector-base-1.18.1.jar \
        "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-base/1.18.1/flink-connector-base-1.18.1.jar"

WORKDIR /opt/flink/usrlib

COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

COPY src/ /opt/flink/usrlib/src/
COPY config/ /opt/flink/usrlib/config/
COPY docker/conf/flink-conf.yaml /opt/flink/conf/flink-conf.yaml

ENV PYTHONPATH=/opt/flink/usrlib/src:/opt/flink/usrlib
ENV FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"

RUN mkdir -p /opt/flink/output /opt/flink/checkpoints /opt/flink/savepoints \
    && chown -R flink:flink /opt/flink/output /opt/flink/usrlib /opt/flink/checkpoints /opt/flink/savepoints

USER flink
