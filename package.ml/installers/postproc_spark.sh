
# stop history server first before creating events
sudo -s  ${SPARK_HOME}/sbin/stop-history-server.sh

# create spark directory for events and warehouse storage
USER_PERM=`echo $(logname)`
mkdir -p ${SPARK_WAREHOUSE_DIR}
mkdir -p ${SPARK_EVENTS_DIR}
sudo  -s chmod 777 ${SPARK_WAREHOUSE_DIR}
sudo  -s chmod 777 ${SPARK_EVENTS_DIR}
sudo  -s chown -R ${USER_PERM}:${USER_PERM} ${SPARK_META_DIR}

# start daemons
sudo -s  ${SPARK_HOME}/sbin/start-history-server.sh
