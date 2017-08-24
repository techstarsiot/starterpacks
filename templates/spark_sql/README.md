## Spark SQL In-Memory Cache Benchmark

### Pre-Requisites
- Copy config/credentials.template.yml to config/credentials.yml and fill in AWS Credentials
- Copy config/schema.template.json to config/schema.json and modify appropriately  

### Defaults
- Default Data structure:
```
    {data_path}/csv
    {data_path}/json
    {data_path}/target
```

### Installation per AWS Instance
1. Launch and Configure AWS Ubuntu 14.04 Server Instance
```
Open the following ports via Security Rules:
    - 18080
    - 4040
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
sudo -s ./make_installers.sh
```
7. add configuration to your $HOME/.bashrc file and execute source the file
```
source $HOME/config/dotfiles/dotconfig_extensions
source $HOME/.bashrc
```
- edit the configuration files in config directory {schema.json, credentials.yml} appropriately

sudo chmod 777 /home/ubuntu/spark/meta/spark-warehouse
mkdir /tmp/spark-events
sudo ln -s /tmp/spark-events /home/ubuntu/spark/meta/spark-events/


### Usage
```sh
# perform in-memory cache + write/read back from S3 + convert to Pandas
# override default name of output directory/file to S3
# override default path to S3 (S3a)
spark-submit sparksql_workflow.py -bn techstars.iot.dev -bp data
```

```sh
# perform only write/read from S3 without in-memory cache
spark-submit sparksql_workflow.py -b -bn techstars.iot.dev -bp data
```

```sh
# perform sql post query: basic example currently
spark-submit sparksql_workflow.py --sql "SELECT id, latitude"
```

```
# override default src path (or given file name)
spark-submit sparksql_workflow.py --src "s3a://techstars.iot.dev/data/csv/<file name>"
```


#### Benchmarks
- View Benchmarks per:
    - Ingest data
    - Hive In-Memory Cache of Ingested data
    - Write/Read S3
    - Convert from S3 Parquet File(s) to DataFrame or JSON output

#### limitations
- Singleton `Session Context`, for multiple contexts - requires database per metastore hive table


### Spark UI
- For currently active executing program (hence blocking program from finishing)
- http://localhost:4040

### History Server
- For viewing actively running sessions and past sessions
- Execute `$SPARK_HOME/sbin/start-history-server.sh` at command line (if not already started, dotfiles start by default)
- http://localhost:18080
