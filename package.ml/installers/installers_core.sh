
apt-get update -y

### Get Java JDK
 apt-get update -y \
 && apt-get install -y software-properties-common \
 && add-apt-repository -y ppa:webupd8team/java \
 && apt-get update -y \
 && echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections \
 && apt-get install -y oracle-java8-installer

### Get SBT for Scala
echo "deb https://dl.bintray.com/sbt/debian /" | tee -a /etc/apt/sources.list.d/sbt.list \
&& apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823 \
&& apt-get update  -y \
&& apt-get install -y sbt
# remove generated files by SBT
find . -name target -type d -exec rm -rf {} \

### Get Maven
wget http://apache.mirrors.tds.net/maven/maven-3/${MAVEN_VERSION}/binaries/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
&& tar -xvzf apache-maven-${MAVEN_VERSION}-bin.tar.gz -C ${EXTERNAL_DEST_DIR} \
&& rm apache-maven-${MAVEN_VERSION}-bin.tar.gz

### Get Anaconda (Version 3)
# Default environment as Anaconda Python 3
wget https://repo.continuum.io/archive/Anaconda3-4.4.0-Linux-x86_64.sh \
&& bash Anaconda3-4.4.0-Linux-x86_64.sh -b -p ${ANACONDA3_PATH}  \
&& conda update  -y anaconda \
&& conda upgrade -y conda \
&& rm Anaconda3-4.4.0-Linux-x86_64.sh \
&& conda clean -yt
# Create Base Environments
conda env create -f ${CONFIG_DIR}/anaconda/${ANACONDA_ENV_IMPORT}_env.yaml
export PATH=${ANACONDA3_PATH}/envs/${ANACONDA_ENV_IMPORT}/bin:$PATH
conda update anaconda
conda update conda
pip install upgrade --pip





