# Define Versions foro Env Installation
export SCALA_VERSION=2.10.4
export SPARK_VERSION=2.2.0
export SPARK_HADOOP_VERSION=2.7
export HADOOP_VERSION=2.7.2
export HIVE_VERSION=2.1.1

# Define Spark Env
export SPARK_HOME=${EXTERNAL_DEST_DIR}/spark
export PYTHONPATH=${SPARK_HOME}/bin:$PATH
# Define Hadoop Env
export HADOOP_HOME=${EXTERNAL_DEST_DIR}/hadoop-${HADOOP_VERSION}
export HADOOP_CONF=${HADOOP_HOME}/etc/hadoop/
export HADOOP_CONF_DIR=${HADOOP_CONF}
export HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_HOME}/lib/native
export HADOOP_OPTS="$HADOOP_OPTS -Djava.net.preferIPv4Stack=true"
export HADOOP_OPTS="$HADOOP_OPTS -Djava.library.path=$HADOOP_HOME/lib"
export LD_LIBRARY_PATH=${HADOOP_HOME}/lib/native/:$LD_LIBRARY_PATH
# Define Hive Env
export HIVE_HOME=${EXTERNAL_DEST_DIR}/apache-hive-$HIVE_VERSION-bin

# setup to create temporary directories on boot, needs to match spark-defaults
# to minimize storage, this is done on boot but can be modified via spark-defaults
export SPARK_META_DIR=$HOME/spark/meta
export SPARK_WAREHOUSE_DIR=$SPARK_META_DIR/spark-warehouse
export SPARK_EVENTS_DIR=${SPARK_META_DIR}/spark-events

# setup PATH environments
export PATH=${SPARK_HOME}/bin:$PATH
export PATH=${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${PATH}
export PATH=$HIVE_HOME/bin:$PATH
