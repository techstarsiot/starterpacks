source ./config_base.sh  

source ./config_core.sh  
source ./installers_core.sh 
source ./postproc_core.sh

source ./config_spark.sh
source ./installers_spark.sh
source ./postproc_spark.sh

# add to bash profile
cp -avf ./dotconfig_extensions ${CONFIG_DIR}/dotfiles
