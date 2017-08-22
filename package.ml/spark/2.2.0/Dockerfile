FROM akamlani/package-anaconda-base:master

WORKDIR /root

RUN apt-get update
ENV SCALA_VERSION=2.10.4
ENV SPARK_VERSION=2.2.0
ENV SPARK_HADOOP_VERSION=2.7
ENV HADOOP_VERSION=2.7.2
ENV HIVE_VERSION=2.1.1

# Get Spark Version
RUN \
    wget https://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz \
    && tar xvzf spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz \
    && rm spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz

# Get Hadoop Version
RUN \
    wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar xvzf hadoop-${HADOOP_VERSION}.tar.gz \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# Get Hive Version
RUN \
    wget http://mirror.metrocast.net/apache/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
    && tar xzvf apache-hive-${HIVE_VERSION}-bin.tar.gz \
    && rm  apache-hive-${HIVE_VERSION}-bin.tar.gz


# Define Spark Env
RUN ln -s /root/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION} /root/spark
ENV SPARK_HOME=/root/spark
ENV PATH=${SPARK_HOME}/bin:$PATH
ENV PYTHONPATH=${SPARK_HOME}/bin:$PATH

# Define Hadoop Env
ENV HADOOP_HOME=/root/hadoop-${HADOOP_VERSION}
ENV HADOOP_CONF=${HADOOP_HOME}/etc/hadoop/
ENV HADOOP_CONF_DIR=${HADOOP_CONF}
ENV HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_HOME}/lib/native
ENV HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true"
ENV HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib"
ENV PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}
ENV LD_LIBRARY_PATH=${HADOOP_HOME}/lib/native/:$LD_LIBRARY_PATH

# Define Hive Env
ENV HIVE_HOME=/root/apache-hive-$HIVE_VERSION-bin
ENV PATH=$HIVE_HOME/bin:$PATH

# Copy Default Configurations
COPY config/spark/ config/spark
COPY config/spark/ ${SPARK_HOME}/conf/

# make system directories
RUN mkdir -p /tmp/spark-events
RUN mkdir -p /tmp/spark-warehouse

# start daemons
RUN ${SPARK_HOME}/sbin/start-history-server.sh

# Expose any particular ports for services
# Ports: {80: http, 8888: nb, 50070: datanode, 8080: worker, 4040: spark ui, 18080: spark history server}
EXPOSE 80 8888 50070 8080 4040 18080

# Set default entrypoint
ENTRYPOINT ["/bin/bash"]
