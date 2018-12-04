#!/usr/bin/env python3

import boto3
import botocore
import json
import os
from  pygments import highlight, lexers, formatters
import argparse
import configparser
#from boto3.s3.connection import S3Connection
from multiprocessing.pool import ThreadPool

#filenames = ['01.json', '02.json', '03.json', '04.json', '05.json', '06.json', '07.json', '08.json', '09.json', '10.json']
#filenames = ['11.json', '12.json', '13.json', '14.json', '15.json', '16.json', '17.json', '18.json', '19.json', '20.json']


def get_s3_objectnames(bucket, prefix):
    s3client = boto3.client('s3')
    object_list = s3client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    object_names = []
    for name in object_list['Contents']:
        file = name['Key']
        file = file[len(prefix + '/'):]
        object_names.append(file)

    result = json.dumps(object_names,
                        sort_keys=True,
                        indent=2)

    print(highlight(
              result,
              lexers.JsonLexer(),
              formatters.TerminalFormatter()
    ))

    return object_names


def put_s3_object(myfile):
    s3 = boto3.resource('s3')
    assetId = os.path.splitext(myfile)[0]
    key = bucket_prefix + "/" + assetId + '/' + myfile

    try:
        s3.Object(bucket_name, key).load()
    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == '404':
            # the object doesn't already exist, so upload it
            myfile = '01.json'
            print('Uploading file "{}", to S3 location "{}/{}"'.format(myfile,
                                                                       bucket_name,
                                                                       key
                                                                       ))
            s3.Object(bucket_name, key).put(Body=open('./{}'.format(myfile), 'rb'))
        else:
            print("I got an unexpected error code from S3: {}".format(err.response['Error']['Code']))
    else:
        # the object already exists, so do not upload it
        print("Doing nothing, the Object already exisits: {}".format(key))


#    print('bucket key is: ' + key)
#    return myfile


def get_ddb_object_names(table_name, attributes):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    object_names = table.scan(
            ProjectionExpression='assetId, assetPrefix'
            )
    object_names = []
    for objectname in objectnames['Items']:
        object_names.append(objectname['itemname'])

    result = json.dumps(object_names,
                        sort_keys=True,
                        indent=2)

    print(highlight(
              result,
              lexers.JsonLexer(),
              formatters.TerminalFormatter()
    ))

    return object_names


def main():
    """Entrypoint to use as command."""
    parser = argparse.ArgumentParser(
        description="Read config ini file",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        usage='%(prog)s $env.ini'
        )
    parser.add_argument("inifile", help='''
        .ini file for configuration settings.
    ''')
    parser.parse_args()
    args = parser.parse_args()
    config = configparser.RawConfigParser()
    # Preserve key case, e.g., for QueueNames
    config.optionxform = lambda option: option
    config.read(args.inifile)
    global bucket_name, bucket_prefix

    # Read configuration values
    bucket_name = config['AWS']['bucket_name']
    bucket_prefix = config['AWS']['bucket_prefix']
    table_name = config['AWS']['table_name']
    table_attributes = config['AWS']['table_attributes']

    print("bucket name is : {}".format(bucket_name))
    print("bucket prefix is : {}".format(bucket_prefix))
    #filenames = get_s3_objectnames(bucket_name, bucket_prefix)

    print("table name is : {}".format(table_name))
    for attributes in table_attributes.split(','):
        print("table attributes are : {}".format(attributes))
    filenames = get_ddb_object_names(table_name, table_attributes)

    pool = ThreadPool(processes=6)
    pool.map(put_s3_object, filenames)


if __name__ == "__main__":
    main()

