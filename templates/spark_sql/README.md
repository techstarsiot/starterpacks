## Spark SQL In-Memory Cache Benchmark
The following modules and scripts represent a way to benchmark your
stages of the pipeline using a distributed technology, e.g. Apache Spark
via Spark SQL and DataFrames.


### Defaults
- Default Data structure:
```
    {data_path}/csv
    {data_path}/json
    {data_path}/target
```

### Installation per AWS Instance
- Reference the [Installer Instructions](./INSTALLER.md) for the
public AMI, as well as steps


### Usage
Script: sparksql_workflow.py
First level script for ingestion of data to perform queries on, in addition
performs transactions to write/read S3.


```sh
# overides the default source path for the given file
# --appname: unique name to monitor the stages performed for a test sequence
# --src: overrides the default source path to ingest data from
# -mem: memory size for driver (in GB)
# -tab: performs creation of a hive table
# -bkt: performs write and read to S3 (s3a) from a hive table
# -bn:  overrides default bucket name to use for S3
# -bp:  overrides default bucket path to use
# -tr:  for transforms to pandas and json (assuming data is small enough in memory)

spark-submit sparksql_workflow.py -mem 20g -tab -bkt
-tgt "sample_target"
-bn  "techstars.iot.dev"
-bp  "data"
-mem "20g"
--appname "Benchmark.experiment.001"
--src "s3a://techstars.iot.dev/data/sample_data.csv"
```

Script: sparksql_partitions.py
Performs Partitioning based on given keys from hive metastore table to
store locally on disk, create an archived file and write/read to S3.



```sh
# -bn:  overrides default bucket name to use for S3
# -bp:  overrides default bucket path to use
# -mem: memory size for driver (in GB)
# -tbl: name of local table for partitioning
# -tgt: target folder to store within src root
# -src: src root path to export partition tables
# --appname: unique name to monitor the stages performed for a test sequence

spark-submit sparksql_partitions.py
-bn  "techstars.iot.dev"
-bp  "ckpts"
-mem "20g"
-tbl "partition_mgr"
-src "/volumes/techstars/ckpts"
-tgt "partitions"
--appname "Benchmark.experiment.001"
```


#### Benchmarks
- View Benchmarks per:
    - Ingest data
    - Hive In-Memory Cache of Ingested data
    - Write/Read S3
    - Partitioning based on given keys and queries
    - Convert from S3 Parquet File(s) to DataFrame or JSON output

#### limitations
- Singleton `Session Context`, for multiple contexts - requires database per metastore hive table
- History Server permissions to view remotely - reference below

### Spark UI
- For currently active executing program (hence blocking program from finishing)
- http://<IP>:4040

### History Server
- For viewing actively running sessions and past sessions (identified by app id and name)
- You will have to edit the permissions as you generate new history logs to view remotely,
since we are not running with a dedicated `spark user and group`.  Possibly with a cron job.
```sh
sudo chmod +r -R /tmp/spark-events/
```
- Execute `$SPARK_HOME/sbin/start-history-server.sh` at command line (if not already started, dotfiles start by default)
- http://<IP>:18080
