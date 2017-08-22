
mkdir -p ${CONFIG_DIR}/anaconda

# define environment variables/versions
export MAVEN_VERSION=3.3.9

export ANACONDA3_PATH=${HOME}/anaconda3
export PATH=${ANACONDA3_PATH}/bin:$PATH
export ANACONDA_ENV_IMPORT=techstarsiot.base.3.6

