#!/usr/bin/env python3

import boto3
import botocore
import json
import os
from functools import partial
from random import random
from time import sleep
from pygments import highlight, lexers, formatters
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


def put_s3_object(filenames_tuple, pool_workers, max_index):
    index, myfile = filenames_tuple
    global total_bytes_uploaded

    s3 = boto3.resource('s3')
    assetId = os.path.splitext(myfile)[0]
    key = bucket_prefix + "/" + assetId + '/' + myfile

    max_percent = 100
    total_bytes_uploaded[index] = {}
    total_bytes_uploaded[index][assetId] = 0

    current_values = [Value(0) for i in range(max_index)]
    default_values = dict(type=Bar, kwargs=dict(max_value=max_percent))
    progress_bar = Bar(title=assetId, max_value=max_percent, fallback=True)
    progress_bar_title = "Total Upload Progress"
    progress_tree = {
            "Total Upload Progress": {
                }
            }

    def increment_values(obj):
        for val in current_values:
            if val.value >= max_percent:
                del progress_tree[progress_bar_title][index]
                break
            val.value += 1

    def done_yet(obj):
        return all(val.value == max_percent for val in current_values)

    def multi_progress_tree(progress_tree):
        progress_tree[progress_bar_title][index] = {
            myfile: BarDescriptor(
                value=current_values[index], **default_values
                )
        }

    def multiple_uploads_progress_tree(current_value):

        multi_progress_tree(progress_tree)

        t = Terminal()
        status_output = ProgressTree(term=t)

        # Create space in the termnial for the status chart
        status_output.make_room(progress_tree)

        while not done_yet(progress_tree):
            sleep(0.2 * random())
            status_output.cursor.restore()
            increment_values(progress_tree)
            status_output.draw(progress_tree, BarDescriptor(default_values))

    def single_uploads_progress_bar(current_value):
        progress_bar.cursor.restore()
        progress_bar.draw(value=current_value)

    def get_percent_done(bytes_uploaded):
            file_size = os.stat(myfile).st_size
            total_bytes_uploaded[index][assetId] = total_bytes_uploaded[index][assetId] + bytes_uploaded
            percent_done = int((total_bytes_uploaded[index][assetId] / file_size) * 100)
            if pool_workers > 1:
                #print("running multiple progress")
                multiple_uploads_progress_tree(percent_done)
            else:
                #print("running single progress")
                single_uploads_progress_bar(percent_done)

    try:
        s3.Object(bucket_name=bucket_name, key=key).load()
    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == '404':
            # the object doesn't already exist, so upload it
            myfile = '01.json'
            #print('Uploading object index {}, filename "{}", '
            #      ' to S3 location "{}/{}"'.format(index, myfile,
            #                                       bucket_name, key
            #                                       ))
            progress_bar.cursor.clear_lines(5)
            progress_bar.cursor.save()

            s3.meta.client.upload_file(
                    myfile,
                    Bucket=bucket_name, Key=key,
                    Callback=get_percent_done
                    )
        else:
            print('I got an unexpected error code '
                  'from S3: {}'.format(err.response['Error']['Code']))
    else:
        # the object already exists, so do not upload it
        print('Doing nothing for object index: {},'
              ' the Object already exisits: {}'.format(index, key))

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
    filenames = ["01.json", "02.json", "03.json", "04.json", "05.json"]
    max_index = len(filenames)

    def pool_function_args(filenames_tuple):
        put_s3_object(filenames_tuple, parallelism, max_index)

    pool = ThreadPool(processes=parallelism)
    pool.map(pool_function_args, enumerate(filenames))


if __name__ == "__main__":

    main()
