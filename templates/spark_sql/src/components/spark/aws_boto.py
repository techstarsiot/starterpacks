import boto3 as boto
import boto3.s3.transfer as tfr

import os 
import sys
import threading

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)\n" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()

def create_aws_conn(access_key, secret_key):
    """
    create boto connection to s3
    """
    resource = boto.resource('s3', region_name='us-east-1')
    client = boto.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    return client

def write_aws_boto(client, bucket_name, bucket_path, source_file):
    """
    write to aws via normal boto interface multiparted
    """
    config = tfr.TransferConfig(
        multipart_threshold=2 * 1024 * 1024,
        max_concurrency=10,
        num_download_attempts=10,
    )
    transfer = tfr.S3Transfer(client, config)
    transfer.upload_file(source_file, bucket_name, bucket_path, callback=ProgressPercentage(source_file))

def read_aws_boto(client, bucket_name, bucket_path, dest_file):
    """
    read an s3 resource via normal boto interface mutliparted   
    """
    config = tfr.TransferConfig(
        multipart_threshold=2 * 1024 * 1024,
        max_concurrency=10,
        num_download_attempts=10,
    )
    transfer = tfr.S3Transfer(client, config)
    transfer.download_file(bucket_name, bucket_path, dest_file)
    
    
