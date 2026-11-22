# Spark 3.5 with the Iceberg runtime and AWS bundle (S3FileIO for MinIO).
# Spark 3.5 is used because dbt-spark is most widely tested against it; Iceberg 1.12 supports 3.5, 4.0 and 4.1.
FROM spark:3.5.8-scala2.12-java17-python3-ubuntu

ARG ICEBERG_VERSION=1.12.0
ARG MAVEN=https://repo1.maven.org/maven2

USER root
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar /opt/spark/jars/
ADD --chmod=644 ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar /opt/spark/jars/
COPY --chmod=644 spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY --chmod=755 jobs/ /opt/spark/jobs/
RUN mkdir -p /landing && chown spark:spark /landing
USER spark
