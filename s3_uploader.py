#!/usr/bin/env python3

import boto3
import json
#from boto3.s3.connection import S3Connection
from multiprocessing.pool import ThreadPool

filenames = ['01.json', '02.json', '03.json', '04.json', '05.json', '06.json', '07.json', '08.json', '09.json', '10.json']
s3 = boto3.resource('s3')
bucket_name = 'devs.bucket'
bucket_prefix= '/'


def get_filenames(bucket, prefix):
    s3 = boto3.resource('s3')
    filenames = bucket.objects.all(Prefix=bucket_prefix)
    json.dumps(filenames)


def upload(myfile):
    key = s3.upload_file(myfile, bucket_name, myfile)
#    print('bucket key is: ' + key)
#    return myfile


pool = ThreadPool(processes=3)
pool.map(upload, filenames)
