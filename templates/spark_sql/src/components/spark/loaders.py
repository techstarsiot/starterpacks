import yaml
import json
import pandas as pd

from pyspark.sql.types import *


### Read aws credentials from configuration file
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

### Read in CSV File as Input for Spark Session DataFrame
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

def read_data(session, fname, schema_fmt, null_val, fmt='csv'):
    """
    read in data from a known source
    assumes data source as hdfs

    null value represents a null value other than missing data
    limitations: currently only supports 'csv' format to replace sql database
    """
    df = session.read \
        .format(fmt) \
        .option('header', 'true') \
        .option("nullValue", null_val) \
        .option('inferSchema', 'false') \
        .schema(schema_fmt) \
        .load(fname)
    return df
