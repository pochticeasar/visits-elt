# Spark + S3A + Python-библиотеки + JupyterLab.
ARG SPARK_VERSION
ARG SCALA_VERSION

FROM apache/spark:${SPARK_VERSION}-scala${SCALA_VERSION}-java17-python3-ubuntu
ARG SPARK_VERSION
ARG HADOOP_VERSION
ARG AWS_SDK_BUNDLE_VERSION
USER root

# S3A: hadoop-aws той же версии, что Hadoop внутри Spark, и его AWS SDK.
RUN cd /opt/spark/jars && M=https://repo1.maven.org/maven2 && \
    wget -q $M/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    wget -q $M/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_BUNDLE_VERSION}/aws-java-sdk-bundle-${AWS_SDK_BUNDLE_VERSION}.jar

RUN pip install --no-cache-dir \
      pyspark==${SPARK_VERSION} pyarrow==17.0.0 pandas==2.2.3 numpy==1.26.4 \
      boto3==1.35.99 matplotlib==3.9.4 jupyterlab==4.3.5

COPY spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
ENV PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3 \
    PYTHONPATH=/opt/jobs \
    PATH=/opt/spark/bin:$PATH

RUN mkdir -p /home/spark /opt/jobs /work && chown -R spark /home/spark /work
ENV HOME=/home/spark
USER spark
WORKDIR /work
