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

import components.spark.partitions  as pt

    from pyspark.sql.types import *
    from pyspark.sql.functions import col


if __name__ == "__main__":
    default_bucket_name = 'techstars.iot.dev'
    default_bucket_path = 'ckpts'
    default_targetname  = 'partitions'

    # parse script
    parser = argparse.ArgumentParser(description="spark sql load metadata")
    parser.add_argument('-app',  '--appname',           default='partition.exp.001',   nargs='?', help='application name')
    parser.add_argument('-tgt',  '--target',            default=default_targetname,    nargs='?', help='associated target output')
    parser.add_argument('-bn',   '--bucketname',        default=default_bucket_name,   nargs='?', help='default s3 I/O name')
    parser.add_argument('-bp',   '--bucketpath',        default=default_bucket_path,   nargs='?', help='associated bucketpath')
    parser.add_argument('-mem',  '--memory',            default="6g",                  nargs='?', help='spark driver memory')
    args = parser.parse_args()
    print(args)

    ### gather credentials from environment
    # alt => set this system wide in $SPARK_HOME/conf/core-site.xml
    creds = ld.read_credentials()

    ### Setup Spark Session
    warehouse_dir = "/tmp/spark-warehouse"#os.getenv('SPARK_WAREHOUSE_DIR')
    if not os.path.exists(warehouse_dir): os.makedirs(warehouse_dir)
    spark = SparkSession \
            .builder \
            .appName(args.appname) \
            .master(utils.acquire_url(utils.MODE_SPARK_LOCAL)) \
            .enableHiveSupport()   \
            .config("spark.sql.warehouse.dir", warehouse_dir) \
            .config("spark.driver.memory", args.memory) \
            .config("spark.executor.memory", "20g") \
            .config("fs.s3a.access.key", creds['aws_access_key_id'])   \
            .config("fs.s3a.secret.key", creds['aws_secret_access_key']) \
            .getOrCreate()
    sc = spark.sparkContext
    print("Spark Version: {}".format(spark.version))
    ### Show Current Tables
    print(spark.catalog.listTables('default'))



#### Temporary until 'Group' Is Ready 
    ### Acquire Cached Hive Table
    table_name, _, _ = ld.load_schema_config(fname='../config/schema.json')
    hive_table_name = "_".join([table_name, "hive"])
    df_hive = spark.table(hive_table_name)
    df_hive.cache()
    print("Cached State: {}".format(df_hive.is_cached))

    ### Perform UDF Repartitioning
    fullRDD = sc.emptyRDD()
    items = df_hive.select("data_url").rdd.flatMap(lambda x: x).collect()
    for link in items:
        rdd = sc.binaryFiles(link).map(lambda x: (link, x))
        fullRDD = fullRDD.union(rdd)

    print( fullRDD.map(lambda x: x[1]).take(2) )
