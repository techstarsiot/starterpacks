

export PATH=${ANACONDA3_PATH}/envs/${ANACONDA_ENV_IMPORT}/bin:$PATH

USER_PERM=`echo $(logname)`
sudo  -s chown -R ${USER_PERM}:${USER_PERM} ${ANACONDA3_PATH}

source activate ${ANACONDA_ENV_IMPORT}
