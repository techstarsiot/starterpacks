import pandas as pd
import numpy  as np
import os
import json
import yaml
import argparse
from pprint import pprint

from pyspark.sql.types import *
from pyspark.sql  import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql  import Row

MODE_SPARK_LOCAL = 'local'
MODE_SPARK_STANDALONE = 'spark_standalone'
MODE_SPARK_CLUSTER = 'spark_cluster'

def acquire_url(mode_operation):
    """
    Acquire url for spark master
    Modes supported: Local, Spark Standalone
    """
    url = None
    if mode_operation == MODE_SPARK_LOCAL: url = "local[*]"
    elif mode_operation == MODE_SPARK_STANDALONE: url = "spark://master:7077"
    return url

def load_schema_config(fname):
    """
    create a dataframe of the schema configuration
    """
    table_name, df_json = (None, None)
    with open(fname, 'r') as f:
        data = json.load(f)
        json_meta  = data['schema']
        table_name = data['table']
        cols_order = data['order']
        null_val   = data['null']
        df_json = pd.DataFrame(json_meta)
        df_json = df_json[cols_order].T
    return table_name, null_val, df_json

def create_spark_schema(df_schema_in):
    """
    create a new spark schema mapping information from the loaded schema config dataframe
    """
    fn_type_map   = {'String': StringType, 'Integer': IntegerType, 'Long': LongType,
                     'Float': FloatType, 'DateTime': TimestampType}
    fn_status_map = {'true': True, 'false': False}
    df_schema_in['type'] = df_schema_in['type'].apply(lambda x: fn_type_map[x])
    df_schema_in = df_schema_in.replace(fn_status_map)
    # map new schema for spark dataframe
    schema_fmt = StructType([StructField(str(k), row.type(), row.nullable) for k, row in df_schema_in.iterrows()])
    return schema_fmt

def read_data(fname, schema_fmt, null_val, fmt='csv'):
    """
    read in data from a known source
    assumes data source as hdfs

    null value represents a null value other than missing data
    limitations: currently only supports 'csv' format to replace sql database
    """
    df = spark.read \
        .format(fmt) \
        .option('header', 'true') \
        .option("nullValue", null_val) \
        .option('inferSchema', 'false') \
        .schema(schema_fmt) \
        .load(fname)
    return df


#create table/hive metastore
def create_table(df_in, name, cache_state=True):
    """
    creates a mapped table view of in memory table with appropriate cache
    """
    df_in.createOrReplaceTempView(name)                        #--- create table
    if cache_state: df_in.cache()                              #--- caches specified table in-memory
    #df_view = spark.table(name)

def create_hive_table(name, hive_name, path_dir, cache_state=True):
    """
    create mapped table in hive metastore
    """
    spark.sql("DROP TABLE IF EXISTS {}".format(hive_name))     #--- remove existing same table in hive
    spark.table(name) \
         .write.option("path", path_dir) \
         .saveAsTable(hive_name)                               #--- save in hive metastore to make queries
    if cache_state: spark.catalog.cacheTable(hive_name)        #--- caches in hive metastore


def evict_cache(name, hive_name=None, evict_all=False):
    """
    remove mapped table(s) from in-memory cache
    note that there may not be an associated hive table mapped if not existing indicated
    """
    if evict_all:
        spark.catalog.clearCache()                             #--- removes all cached tables
    else:
        if name: spark.catalog.uncacheTable(table_name)        #--- uncache table individually
        if hive_name: spark.catalog.uncacheTable(hive_name)    #--- uncache table individually

def get_info(name, hive_name):
    """
    acquire information about db, table metastore
    """
    df_dbs    = pd.DataFrame(spark.catalog.listDatabases())
    df_tables = pd.DataFrame(spark.catalog.listTables())
    table_cache_states = {'table_cache_state': spark.catalog.isCached(name),
                          'hive_cache_state': spark.catalog.isCached(hive_name)}
    return df_dbs, df_tables, table_cache_states



### Setup Tables and Hive Metastore
def get_s3bucket_destination(dest_path, fname, fmt='parquet'):
    """
    get fully complete bucket destination to read/write
    """
    dest_path   = "s3a://{}".format(dest_path)
    fname       = os.path.join(dest_path, ".".join([fname, fmt]))
    return fname

def write_s3bucket(df_in, dest_path, fname, fmt='parquet'):
    """
    write distributed data to s3 bucket
    default format is parquet, but 'csv', or 'json' can be used as well
    """
    fname = get_s3bucket_destination(dest_path, fname, fmt)
    df_in.write.mode('overwrite').format(fmt).save(fname)

    # TBD (binary data, not for dataframe):
    #rdd = sc.textFile(fname)
    #rdd.saveAsTextfile(different_bucket_fname)

def read_s3bucket(dest_path, fname, fmt='parquet'):
    """
    read binary data from s3 bucket
    """
    fname = get_s3bucket_destination(dest_path, fname, fmt)
    df_read = spark.read.format(fmt).load(fname)

    # TBD (binary data, not for dataframe)
    # rdd = sc.textFile(different_bucket_fname)
    return df_read

# read aws credentials
def read_credentials(fname='../config/credentials.yml'):
    """
    get aws credentials from yaml file
    alternatively there are many other methods to get the credentials:
    1. exporting via environment variables from bash
    2.
    """
    credentials=None
    with open(fname, 'r') as f:
        credentials = yaml.load(f)
        credentials = credentials['aws']
    return credentials


def log_jobgroup(log_id, meta_str):
    """
    set identifier and string for job log
    """
    log_id = log_id + 1
    sc.setJobGroup(str(log_id), meta_str)
    return log_id

if __name__ == "__main__":
    evt_id = 0
    default_bucket_name = 'akamlani.vendors.techstars.iot'
    default_bucket_path = os.path.join('vendors', 'sandbox')
    default_bucket_path = os.path.join(default_bucket_name, default_bucket_path)

    default_path  = os.path.join(os.path.join("..", 'data'))
    default_src_path = os.path.join(default_path, 'csv')
    # script level arguments
    parser = argparse.ArgumentParser(description="spark sql workflow")
    parser.add_argument('-bo',   '--bucket', action="store_true", default=False, help='performs only s3 path')
    parser.add_argument('-bn',   '--bucketname',   default="samples",   nargs='?', help='default s3 I/O name')
    parser.add_argument('-bp',   '--bucketpath',   default=default_bucket_path, nargs='?', help='associated bucketpath')
    parser.add_argument('--sql',                   default="", help='performs sql post query, metadata string to be provided')
    parser.add_argument('--src',                   default=default_src_path, nargs='?', help='associated src path or file')
    args = parser.parse_args()
    print(args)

    ### gather credentials from environment
    # alt => set this system wide in $SPARK_HOME/conf/core-site.xml
    creds = read_credentials()
    ### Setup Spark Session
    # create spark session
    warehouse_dir = "/tmp/spark-warehouse"
    if not os.path.exists(warehouse_dir): os.makedirs(warehouse_dir)
    spark = SparkSession \
            .builder \
            .appName("PySpark SQL Benchmark") \
            .master(acquire_url(MODE_SPARK_LOCAL)) \
            .enableHiveSupport()   \
            .config("spark.sql.warehouse.dir", warehouse_dir) \
            .config("fs.s3a.access.key", creds['aws_access_key_id'])   \
            .config("fs.s3a.secret.key", creds['aws_secret_access_key']) \
            .getOrCreate()
    sc = spark.sparkContext
    print("Spark Version: {}".format(spark.version))

    ### Load data and Create Spark Schema
    evt_id = log_jobgroup(evt_id, "Load Schema and Data")
    table_name, null_val, df_schema = load_schema_config(fname='../config/schema.json')
    spark_schema = create_spark_schema(df_schema)
    # args.src could be just a directory or the full qualified filename
    df = read_data(args.src, spark_schema, null_val)
    #df.printSchema()

    if not args.bucket:
        ### Setup Tables and Hive Metastore spark warehouse
        evt_id = log_jobgroup(evt_id, "Create SQL Tables")
        hive_table_name = "_".join([table_name, "hive"])
        create_table(df, table_name)

        evt_id = log_jobgroup(evt_id, "Create Hive Tables")
        create_hive_table(table_name, hive_table_name, warehouse_dir)
        df_dbs, df_tables, table_cache_states = get_info(table_name, hive_table_name)
        #evict_cache(table_name, hive_table_name)

    ### Write/Read hive information to S3
    # Use S3 as Persistent Storage - decouple Compute/Storage by using S3 as data layer
    evt_id = log_jobgroup(evt_id, "Write to S3 Bucket")
    write_s3bucket(df, dest_path=args.bucketpath, fname=args.bucketname)
    evt_id = log_jobgroup(evt_id, "Read from S3 Bucket")
    df_reload = read_s3bucket(dest_path=args.bucketpath, fname=args.bucketname)
    evt_id = log_jobgroup(evt_id, "DataFrame Query upon S3 Reload")
    #df_reload.select('speed', 'altitude').describe().show(5)
    print("S3 Operations Complete")
    ### Convert to normal python usage: occurs by action, no lazy operations
    evt_id = log_jobgroup(evt_id, "Transform to Pandas DF")
    df_pandas = df_reload.toPandas()
    print(df_pandas)


    if not args.bucket and args.sql:
        ### Perform example SQL Statements
        evt_id = log_jobgroup(evt_id, "Query SQL Statement")
        df_query_std  = spark.sql("{query} {table}".format(query=args.sql, table=table_name))
        df_query_hive = spark.sql("{query} {table}".format(query=args.sql, table=hive_table_name))
        df_query_hive_pandas = df_query_hive.toPandas()
        df_query_hive_json   = df_query_hive_pandas.to_json(orient='records')
        df_query_json = json.loads(df_query_hive_json)

        print(df_query_hive_pandas.head())
        pprint(df_query_hive_json, indent=4)
        pprint(df_query_json, indent=4)

    evt_id = log_jobgroup(evt_id, "Blocking Debug Loop")
    while(1):pass
    spark.stop()
