## Spark SQL In-Memory Cache Benchmark

### Pre-Requisites
- Create **data** directory with appropriate csv files at the same level as **src** directory
- Copy config/credentials.template.yml to config/credentials.yml and fill in AWS Credentials
- Copy config/schema.template.json to config/schema.json and modify appropriately  

### Installation
- Sync this repository
- Pull Image via Docker
- Run Script to mount host directory to attach volume to executing container

### Usage
```sh
# perform in-memory cache + write/read back from S3 + convert to Pandas
# override default name of output directory/file to S3
# override default path to S3 (S3a)
spark-submit sparksql_workflow.py -bn test.vendors -bp akamlani.vendors.techstars.iot/vendors/scienceboy
```

```sh
# perform only write/read from S3 without in-memory cache
spark-submit sparksql_workflow.py -b -bn test.vendors -bp akamlani.vendors.techstars.iot/vendors/scienceboy
```

```sh
# perform sql post query: basic example currently
spark-submit sparksql_workflow.py --sql "SELECT id, latitude FROM"
```

```
# override default src path (or given file name)
spark-submit sparksql_workflow.py --src "../data/vendors/demo/data/csv"
```


#### Benchmarks
- View Benchmarks per:
    - Ingest data
    - Hive In-Memory Cache of Ingested data
    - Write/Read S3
    - Convert from S3 Parquet File(s) to DataFrame output

#### limitations
- Singleton `Session Context`, for multiple contexts - requires database per metastore hive table


### Spark UI
- For currently active executing program (hence blocking program from finishing)
- http://localhost:4040

### History Server
- For viewing actively running sessions and past sessions
- Execute `$SPARK_HOME/sbin/start-history-server.sh` at command line (if not already started)
- http://localhost:18080
