import pandas as pd
import numpy  as np
import os
import json
import yaml
import argparse
from pprint import pprint

from pyspark.sql  import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql  import Row

import components.spark.loaders     as ld
import components.spark.utils       as utils
import components.spark.monitor     as dbg
import components.spark.tables      as tbl
import components.spark.aws         as aws


if __name__ == "__main__":
    default_targetname  = 'target_sample'
    default_bucket_name = 'techstars.iot.dev'

    default_bucket_path = 'data'
    default_src_path    = os.path.join(os.path.join("..", 'data'), 'csv')
    default_tgt_path    = os.path.join(os.path.join("..", 'data'), 'target')

    # parse script
    parser = argparse.ArgumentParser(description="spark sql workflow")
   
    parser.add_argument('-app',  '--appname',		    default="benchmark.exp.001",   nargs='?', help='application name')
    parser.add_argument('-tgt',  '--target',            default=default_targetname,    nargs='?', help='associated target output')
    parser.add_argument('-bn',   '--bucketname',        default=default_bucket_name,   nargs='?', help='default s3 I/O name')
    parser.add_argument('-bp',   '--bucketpath',        default=default_bucket_path,   nargs='?', help='associated bucketpath')
    parser.add_argument('--src',                        default=default_src_path,      nargs='?', help='associated src path or file')
    parser.add_argument('--sql',                        default="",                    nargs='?', help='performs simple sql queries')
    parser.add_argument('-mem',  '--memory',            default="6g",                  nargs='?', help='spark driver memory')
    parser.add_argument('-bo',   '--bucketonly',        action="store_true", default=False, help='performs only s3 path')
    parser.add_argument('-tr',   '--transform',         action="store_true", default=False, help='performs transform to pandas')

    args = parser.parse_args()
    print(args)


    ### gather credentials from environment
    # alt => set this system wide in $SPARK_HOME/conf/core-site.xml
    creds = ld.read_credentials()

    ### Setup Spark Session
    warehouse_dir = os.getenv('SPARK_WAREHOUSE_DIR')
    if not os.path.exists(warehouse_dir): os.makedirs(warehouse_dir)
    
    #config("spark.sql.warehouse.dir", warehouse_dir) \
    spark = SparkSession \
            .builder \
            .appName(args.appname) \
            .master(utils.acquire_url(utils.MODE_SPARK_LOCAL)) \
            .enableHiveSupport()   \
	        .config("spark.sql.warehouse.dir", warehouse_dir) \
            .config("spark.driver.memory", args.memory) \
            .config("fs.s3a.access.key", creds['aws_access_key_id'])   \
            .config("fs.s3a.secret.key", creds['aws_secret_access_key']) \
            .getOrCreate()
    sc = spark.sparkContext
    print("Spark Version: {}".format(spark.version))

    ### Load data and Create Spark Schema
    evt_id = 0
    evt_id = dbg.log_jobgroup(sc, evt_id, "Load Schema and Data")
    table_name, null_val, df_schema = ld.load_schema_config(fname='../config/schema.json')
    spark_schema = ld.create_spark_schema(df_schema)
    # args.src could be just a directory or the full qualified filename
    df = ld.read_data(spark, args.src, spark_schema, null_val)
    #df.printSchema()

    ### Setup Tables and Hive Metastore spark warehouse
    target_path = os.path.join(args.bucketpath, 'target')
    if not args.bucketonly:
        evt_id = dbg.log_jobgroup(sc, evt_id, "Create SQL Tables")
        hive_table_name = "_".join([table_name, "hive"])
        tbl.create_table(spark, df, table_name, warehouse_dir)

        evt_id = dbg.log_jobgroup(sc, evt_id, "Create Hive Tables")
        tbl.create_hive_table(spark, table_name, hive_table_name, warehouse_dir)
        df_dbs, df_tables, table_cache_states = tbl.get_info(spark, table_name, hive_table_name)
        #evict_cache(table_name, hive_table_name)

        # Use S3 as Persistent Storage - decouple Compute/Storage by using S3 as data layer
        evt_id = dbg.log_jobgroup(sc, evt_id, "Write to S3 Bucket")
        aws.write_s3bucket(spark, df, bucket_name=args.bucketname, bucket_path=target_path, target=args.target)
        print("S3 Write Operations Complete")

    ### Read from S3 Bucket
    evt_id = dbg.log_jobgroup(sc, evt_id, "Read from S3 Bucket")
    df_reload = aws.read_s3bucket(spark, bucket_name=args.bucketname, bucket_path=target_path, target=args.target)
    print("S3 Read Operations Complete")
    #df_reload.select('speed', 'altitude').describe().show(5)

    ### Convert to normal python usage: occurs by action, no lazy operations
    # assumes the data is small enough to graph the entire data in memory 
    if args.transform:
        evt_id = dbg.log_jobgroup(sc, evt_id, "Transform to Pandas DF")
        df_pandas = df_reload.toPandas()
        print(df_pandas.head())

    ### Perform SQL Aggregations
    if not args.bucketonly and args.sql:
        ### Perform example SQL Statements
        evt_id = dbg.log_jobgroup(sc, evt_id, "Query SQL Statement")
        df_pandas, df_json = tbl.sql_query(spark, args.sql, table_name)
        df_pandas_hive, df_json_hive = tbl.sql_query(spark, args.sql, hive_table_name)
        print(df_pandas.head())
        pprint(df_json, indent=4)

    ### Per debugging
    #evt_id = dbg.log_jobgroup(sc, evt_id, "Blocking Debug Loop")
    #while(1):pass
    spark.stop()
