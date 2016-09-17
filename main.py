# -*- coding: utf-8 -*-

import json
import os

import botocore
import boto3


class SyncClient(object):
    def __init__(self, client, bucket, prefix):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.get_sync_index()

    def get_sync_index(self):
        try:
            data = self.client.get_object(
                Bucket=self.bucket,
                Key=os.path.join(self.prefix, '.syncindex'),
            )['Body']
            json_data = data.read().decode('utf-8')
            self.sync_index = json.loads(json_data)
        except botocore.exceptions.ClientError:
            self.sync_index = {}

    def put_sync_index(self):
        self.client.put_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, '.syncindex'),
            Body=json.dumps(self.sync_index),
        )

    def put_object(self, key, data, time_modified):
        self.sync_index[key] = time_modified
        self.client.put_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
            Body=data,
        )


def main():
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('--local')
    parser.add_argument('--bucket')
    parser.add_argument('--prefix')

    args = parser.parse_args()

    client = boto3.client('s3')
    sync_client = SyncClient(client, args.bucket, args.prefix)

    sync(sync_client, args.local)


def sync(sync_client, local_dir):
    local_index = get_local_index(local_dir)
    perform_sync(sync_client, local_dir, local_index)


def get_local_index(local_dir):
    result = {}
    for filename in os.listdir(local_dir):
        absolute_path = os.path.join(local_dir, filename)
        filestat = os.stat(absolute_path)
        result[filename] = filestat.st_mtime
    return result


def perform_sync(sync_client, local_dir, local_index):
    for key in local_index:
        s3_timestamp = sync_client.sync_index.get(key)
        local_timestamp = local_index[key]
        local_path = os.path.join(local_dir, key)

        if s3_timestamp is None:
            print('Need to upload (CREATE)', key)
            with open(local_path, 'rb') as fp:
                sync_client.put_object(key, fp, local_timestamp)
        elif local_timestamp > s3_timestamp:
            print('Need to upload (UPDATE)', key)
            with open(local_path, 'rb') as fp:
                sync_client.put_object(key, fp, local_timestamp)
        else:
            print('No need to update', key)

    sync_client.put_sync_index()


if __name__ == '__main__':
    main()
