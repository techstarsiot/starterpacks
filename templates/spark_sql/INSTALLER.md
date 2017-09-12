### Public AMI

1. Clone/Launch Techstars Public AMI (AMI's are Region Specific)
```
AMI ID: ami-0c908c77            (us-east-1)
Canada AMI ID: ami-20d16844     (ca-central-1)
```

2. ssh login (without pem file )
```
username: ubuntu
password: ubuntu
```


### Installer Instructions on top of existing Infrastructure
1. Launch and Configure AWS Ubuntu 14.04 Server Instance
```
Open the following ports via Security Policy Rules when creating your EC2 Instance:
    - 18080 (Spark History Server)
    - 4040  (Spark UI)
    - 8888  (optional: Jupyter Notebook)
    - 22    (SSH)
```
2. Execute the following to update and obtain git
``` sh
sudo apt-get update -y
sudo apt-get install git
```
3. copy ssh key from your github
```
scp -i ~/.ssh/<file.pem> ~/.ssh/id_rsa ubuntu@<IP>:/home/ubuntu/.ssh/id_rsa
```
4. clone the repository
```sh
# e.g. clone to $HOME/projects
git clone git@github.com:techstarsiot/starterpacks.git
```
5. make soft links to cloned config directories
```sh
mkdir -p $HOME/config
ln -s $HOME/projects/starterpacks/package.ml/anaconda/base/config/anaconda $HOME/config/anaconda
ln -s $HOME/projects/starterpacks/package.ml/spark/2.2.0/config/spark $HOME/config/spark
ln -s $HOME/projects/starterpacks/package.ml/installers $HOME/installers
```
6. run installers from $HOME/installers
```sh
sudo -s ./make_install.sh
```
7. add configuration to your $HOME/.bashrc file and execute source the file
```
source $HOME/config/dotfiles/dotconfig_extensions
source $HOME/.bashrc
```
- edit the configuration files in config directory {schema.json, credentials.yml} appropriately
    - Copy config/credentials.template.yml to config/credentials.yml and fill in AWS Credentials
    - Copy config/schema.template.json to config/schema.json and modify appropriately from the template  


### For S3 Buckets based out of different regions (e.g. ca-central-1)
Per the following jar file `com.amazonaws_aws-java-sdk-1.7.4.jar`:
- Modify the awssdk_config_default.json  `signerType` field from `S3SignerType`, to `AWSS3V4SignerType`.

```
{
    "key" : "s3",
    "config" : {
    "signerType" : "AWSS3V4SignerType"
}
```

1. cd ~/.ivy2/jars
2. jar xf com.amazonaws_aws-java-sdk-1.7.4.jar awssdk_config_default.json
3. vi awssdk_config_default.json (edit per `signerType` as above)
4. jar xf com.amazonaws_aws-java-sdk-1.7.4.jar awssdk_config_default.json
5. rm ~/.ivy2/jars/awssdk_config_default.json
6. rm -r ~/.ivy2/cache


- Additionally you may find that you need to modify the [endpoint](http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region) configuration.
