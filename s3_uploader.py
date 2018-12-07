#!/usr/bin/env python3

import boto3
import botocore
import json
import os
from random import random
from time import sleep
from  pygments import highlight, lexers, formatters
import argparse
import configparser
from multiprocessing.pool import ThreadPool
from progressive.bar import Bar

from blessings import Terminal
from progressive.tree import ProgressTree, Value, BarDescriptor

total_bytes_uploaded = {}


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
    total_bytes_uploaded[key] = 0
    max_value = 100
    progress_bar = Bar(title=key, max_value=max_value, fallback=True)

    progress_bar.cursor.clear_lines(5)
    progress_bar.cursor.save()

    def simple_tree(current_value):
        max_value = 100
        current_values = [Value(0) for i in range(2)]
        default_values = dict(type=Bar, kwargs=dict(max_value=max_value))

        progress_tree = {}

        for upload_object in filenames:
            progress_tree["Total Upload Progress"] = {
                upload_object: BarDescriptor(value=current_values[0], **default_values)
            }

        t = Terminal()
        status_output = ProgressTree(term=t)

        # Create space in the termnial for the status chart
        status_output.make_room(progress_tree)

        def increment_values(obj):
            for val in current_values:
                if val.value < max_value:
                    val.value += 1
                    break

        def done_yet(obj):
            return all(val.value == max_value for val in current_values)

        while not done_yet(progress_tree):
            sleep(0.2 * random())

    def simple_bar(current_value):
        progress_bar.cursor.restore()
        progress_bar.draw(value=current_value)

    def get_percent_done(bytes_uploaded):
            file_size = os.stat(myfile).st_size
            global total_bytes_uploaded
            total_bytes_uploaded[key] = total_bytes_uploaded[key] + bytes_uploaded
            percent_done = int((total_bytes_uploaded[key] / file_size) * 100)

            simple_bar(percent_done)

    try:
        s3.Object(bucket_name=bucket_name, key=key).load()
    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == '404':
            # the object doesn't already exist, so upload it
            myfile = '01.json'
            print('Uploading file "{}", to S3 location "{}/{}"'.format(myfile,
                                                                       bucket_name,
                                                                       key
                                                                       ))
            s3.meta.client.upload_file(
                    myfile,
                    Bucket=bucket_name, Key=key,
                    Callback=get_percent_done
#                    Callback=tree
                    )
        else:
            print("I got an unexpected error code from S3: {}".format(err.response['Error']['Code']))
    else:
        # the object already exists, so do not upload it
        print("Doing nothing, the Object already exisits: {}".format(key))

    sleep(random())


def get_ddb_object_names(table_name, attributes):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    objectnames = table.scan(
            ProjectionExpression=attributes
            )
    object_names = []
    for objectname in objectnames['Items']:
        object_names.append(objectname[attributes.split(',')[0]])

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
    parallelism = int(config['Python']['parallelism'])
    bucket_name = config['AWS']['bucket_name']
    bucket_prefix = config['AWS']['bucket_prefix']
    table_name = config['AWS']['table_name']
    table_attributes = config['AWS']['table_attributes']

    print("bucket name is : {}".format(bucket_name))
    print("bucket prefix is : {}".format(bucket_prefix))
#    filenames = get_s3_objectnames(bucket_name, bucket_prefix)

    print("table name is : {}".format(table_name))
    for attributes in table_attributes.split(','):
        print("table attributes are : {}".format(attributes))
#    filenames = get_ddb_object_names(table_name, table_attributes)
#    filenames = ["01.json"]
    filenames = ["01.json", "02.json"]

    pool = ThreadPool(processes=parallelism)
    pool.map(put_s3_object, filenames)


if __name__ == "__main__":

    main()
