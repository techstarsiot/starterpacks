from pyspark.sql.functions import udf
from pyspark.sql.functions import col
from pyspark.sql.functions import date_format
from pyspark.sql.functions import to_date
from pyspark import StorageLevel

import os

def create_file_partitions(df, target_path, *partition_id, repartition=False):
    """
    perform the paritioning at a file level basis directory structure to a given target path
    - .coalesce will avoid the full shfuffle that exists with .repartition
    - use 'append' mode, rather than 'overwrite', assuming that script is not run multiple times
    """
    if repartition:
        df_part = df.repartitions(*partition_id)
    else:
        num_partitions = df.rdd.getNumPartitions()
        df_part = df.coalesce(num_partitions)

    df_part \
        .write.mode('append')       \
        .partitionBy(*partition_id) \
        .option("header", True)     \
        .format("parquet")          \
        .save(target_path)


def create_table_partitions(session, df, target_table, *partition_id):
    """
    let parquet discover teh partition method, which will be more efficient and then just perform queries
    just format for the discover process   
    """
    session.sql("DROP TABLE IF EXISTS {}".format(target_table))
    df.write \
      .partitionBy(*partition_id)                       \
      .mode('overwrite')                                \
      .option("header", True)                           \
      .format("parquet")                                \
      .saveAsTable(target_table)


def export_table_partitions(session, input_table, target_path, format_ext, *partition_id):
    """
    export hive partitioned table to directory structure of appropriate format (e.g. csv)
    """
    col_und  = lambda s: "_" + s
    k1, k2   = col_und(partition_id[0]), col_und(partition_id[1])
    df_table = session.table(input_table).persist(StorageLevel.MEMORY_AND_DISK_SER)
    df_table_fmt = df_table.withColumn(k1, df_table[partition_id[0]]).withColumn(k2, df_table[partition_id[1]])
    
    df_table_fmt.repartition(k1, k2)    \
            .write.partitionBy(k1, k2)  \
            .option('header', True)     \
            .mode('overwrite')          \
            .format(format_ext)         \
            .save(target_path)

def query(session, target_path, format_ext, items):
    """
    perform query on partitions to extract the correct partitioned files
    @param: items is a list of tuples of format[(partition_key, query_value), ...]
    """
    col_und = lambda s: "_" + s
    partition_queries = ["=".join([col_und(k),v]) for k,v in items]
    query_str = os.path.join(*partition_queries)
    target_query_str = os.path.join(target_path, query_str)
 
    print("loading from: {}".format(target_query_str))   
    # perform lazy read, call converts to spark or pandas 
    df = session.read                   \
         .option("header", True)        \
         .format(format_ext)            \
         .load(target_query_str)    
    return df 

