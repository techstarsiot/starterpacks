# create spark directory for events and warehouse storage
mkdir -p ${SPARK_WAREHOUSE_DIR}
sudo chmod 777 ${SPARK_WAREHOUSE_DIR}
mkdir -p /tmp/spark-events
sudo ln -s /tmp/spark-events ${SPARK_EVENTS_DIR}

# start daemons
sudo -s  ${SPARK_HOME}/sbin/stop-history-server.sh
sudo -s  ${SPARK_HOME}/sbin/start-history-server.sh
