import pandas as pd
import os
import yaml


### Setup Tables and Hive Metastore
def get_s3bucket_destination(bucket_name, bucket_path, target):
    """
    get fully complete bucket destination to read/write
    """
    dest_path   = os.path.join(bucket_name, bucket_path)
    dest_path   = "s3a://{}".format(dest_path)
    fname       = os.path.join(dest_path, target)
    return fname

def write_s3bucket(session, df_in, bucket_name, bucket_path, target, fmt='parquet'):
    """
    write distributed data to s3 bucket
    default format is parquet, but 'csv', or 'json' can be used as well
    """
    fname = get_s3bucket_destination(bucket_name, bucket_path, target)
    df_in.write.mode('overwrite').format(fmt).save(fname)

    # TBD (binary data, not for dataframe):
    #rdd = sc.textFile(fname)
    #rdd.saveAsTextfile(different_bucket_fname)

def read_s3bucket(session, bucket_name, bucket_path, target, fmt='parquet'):
    """
    read binary data from s3 bucket
    """
    fname = get_s3bucket_destination(bucket_name, bucket_path, target)
    df_read = session.read.format(fmt).load(fname)

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
