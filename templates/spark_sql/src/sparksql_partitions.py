import pandas as pd
import numpy  as np
import os
import json
import yaml
import argparse
from pprint import pprint
from datetime import datetime

from pyspark.sql            import SparkSession
from pyspark.conf           import SparkConf
from pyspark.sql            import Row
from pyspark.sql.functions  import col
from pyspark                import StorageLevel

from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import date_format
from pyspark.sql.functions import to_date

import components.spark.loaders     as ld
import components.spark.utils       as utils
import components.spark.monitor     as dbg
import components.spark.tables      as tbl
import components.spark.aws         as aws
import components.spark.aws_boto    as awsboto

import components.spark.partitions  as pt


if __name__ == "__main__":

    default_bucket_name = 'techstars.iot.dev'
    default_bucket_path = 'ckpts'
    default_targetname  = 'partitions'
    default_tablename   = 'crunch_partitions'
    default_src_root    = '../ckpts'
    default_keys        = "registration, datetime"
    default_queries     = "PH-GGY, 2017-07-30"

    # parse script
    parser = argparse.ArgumentParser(description="spark sql partitions")
    parser.add_argument('-app',  '--appname',           default='partition.exp.001',   nargs='?', help='application name')
    parser.add_argument('-tgt',  '--target',            default=default_targetname,    nargs='?', help='associated target output')
    parser.add_argument('-tbl',  '--table',             default=default_tablename,     nargs='?', help='associated table name')
    parser.add_argument('-src',  '--srcroot',           default=default_src_root,      nargs='?', help='associated local dir structure root')
    parser.add_argument('-bn',   '--bucketname',        default=default_bucket_name,   nargs='?', help='default s3 I/O name')
    parser.add_argument('-bp',   '--bucketpath',        default=default_bucket_path,   nargs='?', help='associated bucketpath')
    parser.add_argument('-mem',  '--memory',            default="6g",                  nargs='?', help='spark driver memory')
    parser.add_argument('-key',  '--keys',              default=default_keys,          nargs='?', help='default partition keys')
    parser.add_argument('-qry',  '--queries',           default=default_queries,       nargs='?', help='default query on file structure')
    args = parser.parse_args()
    print(args)

    part_keys = list( map(lambda x: x.strip(), args.keys.split(',')) )
    queries   = list( map(lambda x: x.strip(), args.queries.split(',')) )
    query_items = list(zip(part_keys, queries))    

    ### gather credentials from environment
    # alt => set this system wide in $SPARK_HOME/conf/core-site.xml
    creds = ld.read_credentials()
    client = awsboto.create_aws_conn(creds['aws_access_key_id'], creds['aws_secret_access_key'])

    ### Setup Spark Session
    warehouse_dir = os.getenv('SPARK_WAREHOUSE_DIR')
    if not os.path.exists(warehouse_dir): os.makedirs(warehouse_dir)
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
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsAccessKeyId",     creds['aws_access_key_id'])
    sc._jsc.hadoopConfiguration().set("fs.s3a.awsSecretAccessKey", creds['aws_secret_access_key'])
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    print("Spark Version: {}".format(spark.version))
    ### Show Current Tables 
    print(spark.catalog.listTables('default'))
    
    ### Acquire Cached Hive Table
    evt_id = 0
    evt_id = dbg.log_jobgroup(sc, evt_id, "Load Data and Hive Table")
    table_name, _, _ = ld.load_schema_config(fname='../config/schema.json')
    
    hive_table_name = "_".join([table_name, "hive"])
    df_hive = spark.table(hive_table_name).persist(StorageLevel.MEMORY_ONLY_SER)
    print("Cached State: {}".format(df_hive.is_cached))
    
    ### Perform UDF Repartitioning
    #evt_id = dbg.log_jobgroup(sc, evt_id, "Filter Data")
    #df_sample = df_hive.sample(False, 0.0001, 42)
    #df_sample.persist(StorageLevel.MEMORY_ONLY_SER)
    
    # for specific test case filtering {registration, datetime}
    df_flt = df_hive.where(col(part_keys[0]).isNotNull())
    df_flt = df_flt.withColumn(part_keys[1], to_date(part_keys[1], "yyyy-MM-dd"))
    df_flt.persist(StorageLevel.MEMORY_ONLY_SER)

    if not os.path.exists(args.srcroot): os.makedirs(args.srcroot)
    partitions_path = os.path.join(args.srcroot, args.target)
    tar_name = ".".join([table_name, 'tar.gz'])
    tar_wr_archives = os.path.join(args.srcroot, "archives")
    if not os.path.exists(tar_wr_archives): os.makedirs(tar_wr_archives)
    tar_wr_archives = os.path.join(tar_wr_archives, tar_name)
    tar_rd_archives = os.path.join(args.srcroot, "s3")
    if not os.path.exists(tar_rd_archives): os.makedirs(tar_rd_archives)
    tar_rd_archives = os.path.join(tar_rd_archives, tar_name)    

    # execute pipeline stages
    evt_id = dbg.log_jobgroup(sc, evt_id, "Create Parquet Partitions")
    pt.create_table_partitions(spark, df_flt, args.table, *part_keys)
    evt_id = dbg.log_jobgroup(sc, evt_id, "Export Dir Structure")
    pt.export_table_partitions(spark, args.table, partitions_path, 'csv', *part_keys)    
    
    tstart = datetime.now()
    evt_id = dbg.log_jobgroup(sc, evt_id, "Create archived file:  {}".format(tar_wr_archives))
    pt.archive_table_partitions(partitions_path, os.path.join(args.srcroot, "archives"), tar_name)
    tstop = datetime.now()
    print("Archive Time Elapsed: {}".format(tstop - tstart))

    # write/read archive file to s3
    # to keep directory structure: we perform via mulipart-boto 
    tstart = datetime.now()
    evt_id = dbg.log_jobgroup(sc, evt_id, "Write archived file to s3:  {}".format(tar_wr_archives))
    awsboto.write_aws_boto(client, args.bucketname, os.path.join(args.bucketpath, tar_name), tar_wr_archives)
    tstop = datetime.now()
    print("S3 Upload Time Elapsed: {}".format(tstop - tstart))    

    tstart = datetime.now()
    evt_id = dbg.log_jobgroup(sc, evt_id, "Read archived file from s3:  {}".format(tar_rd_archives))
    awsboto.read_aws_boto(client,  args.bucketname, os.path.join(args.bucketpath, tar_name), tar_rd_archives)
    tstop = datetime.now()    
    print("Read Archive from S3 Time Elapsed: {}".format(tstop - tstart))
    
    
    # perform query on local structure
    evt_id = dbg.log_jobgroup(sc, evt_id, "Query Results: Convert to Pandas => DF, JSON")
    df_query_result = pt.query(spark, partitions_path, 'csv', query_items)
    df_pandas, df_json = tbl.sql_query_reponse(df_query_result)
    print(df_pandas)
    

    ### Per debugging
    #evt_id = dbg.log_jobgroup(sc, evt_id, "Blocking Debug Loop")
    #while(1):pass
    spark.stop()
    
