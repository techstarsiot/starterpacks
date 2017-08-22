
apt-get update -y

mkdir -p ${CONFIG_DIR}/spark
### Get Spark Version
wget https://d3kbcqa49mib13.cloudfront.net/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz \
&& tar xvzf spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz -C ${EXTERNAL_DEST_DIR} \
&& rm spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}.tgz

### Get Hadoop Version
wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
&& tar xvzf hadoop-${HADOOP_VERSION}.tar.gz -C ${EXTERNAL_DEST_DIR} \
&& rm hadoop-${HADOOP_VERSION}.tar.gz

### Get Hive Version
wget http://mirror.metrocast.net/apache/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
&& tar xzvf apache-hive-${HIVE_VERSION}-bin.tar.gz -C ${EXTERNAL_DEST_DIR} \
&& rm  apache-hive-${HIVE_VERSION}-bin.tar.gz

### Setup easy access to spark
rm -rf ${EXTERNAL_DEST_DIR}/spark
ln -s ${EXTERNAL_DEST_DIR}/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION} ${EXTERNAL_DEST_DIR}/spark
# Copy Default Configurations
cp -avf ${CONFIG_DIR}/spark/* ${SPARK_HOME}/conf/

