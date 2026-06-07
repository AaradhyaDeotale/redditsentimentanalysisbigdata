FROM apache/flink:1.18.1-scala_2.12

USER root

ARG KAFKA_CONNECTOR_VERSION=3.2.0-1.18

RUN apt-get update && apt-get install -y --no-install-recommends \
        python3-pip \
        curl \
        unzip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /opt/flink/lib/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
        "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" \
    && curl -fsSL -o /opt/flink/lib/kafka-clients-3.7.0.jar \
        "https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar" \
    && curl -fsSL -o /opt/flink/lib/flink-connector-base-1.18.1.jar \
        "https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-base/1.18.1/flink-connector-base-1.18.1.jar"

RUN cd /opt/flink/opt/python \
    && unzip -q pyflink.zip -d pyflink_extracted \
    && chmod -R +x pyflink_extracted/pyflink/bin/

WORKDIR /opt/flink/usrlib

COPY docker/requirements-docker.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir --timeout 300 --retries 5 -r /tmp/requirements.txt

COPY src/ /opt/flink/usrlib/src/
COPY config/ /opt/flink/usrlib/config/
COPY docker/conf/flink-conf.yaml /opt/flink/conf/flink-conf.yaml
COPY docker/flink-docker.env /opt/flink/usrlib/.env

ENV PYTHONPATH=/opt/flink/opt/python/pyflink_extracted:/opt/flink/opt/python/pyflink.zip:/opt/flink/usrlib/src:/opt/flink/usrlib
ENV FLINK_PROPERTIES="jobmanager.rpc.address: jobmanager"

RUN mkdir -p /opt/flink/output /opt/flink/checkpoints /opt/flink/savepoints \
    && chown -R flink:flink /opt/flink/output /opt/flink/usrlib /opt/flink/checkpoints /opt/flink/savepoints \
    && chmod -R +x /opt/flink/opt/python/pyflink_extracted/pyflink/bin/

USER flink
