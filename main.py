# -*- coding: utf-8 -*-

import json
import os

import botocore
import boto3

import logging


logger = logging.getLogger(__name__)


class SyncClient(object):
    def __init__(self, client, bucket, prefix):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.get_sync_index()

    def get_sync_index(self):
        logger.info('Getting sync index from %s', self.bucket)
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

    def put_file(self, key, fp, time_modified):
        self.sync_index[key] = time_modified
        self.client.put_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
            Body=fp,
        )

    def get_file(self, key, fp):
        body = self.client.get_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
        )['Body']
        fp.write(body.read())


def main():
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('--local')
    parser.add_argument('--bucket')
    parser.add_argument('--prefix')
    parser.add_argument('--loglevel', default=logging.WARNING)

    args = parser.parse_args()

    logger.addHandler(logging.StreamHandler())
    logger.setLevel(args.loglevel)

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
    all_keys = set(local_index).union(set(sync_client.sync_index))
    for key in all_keys:
        s3_timestamp = sync_client.sync_index.get(key)
        local_timestamp = local_index[key]
        local_path = os.path.join(local_dir, key)

        if s3_timestamp is None:
            logger.info('Need to upload (CREATE): %s', key)
            with open(local_path, 'rb') as fp:
                sync_client.put_file(key, fp, local_timestamp)
        elif local_timestamp is None:
            logger.info('Need to download (CREATE): %s', key)
            with open(local_path, 'wb') as fp:
                sync_client.get_file(key, fp)
        elif local_timestamp > s3_timestamp:
            logger.info('Need to upload (UPDATE): %s', key)
            with open(local_path, 'rb') as fp:
                sync_client.put_file(key, fp, local_timestamp)
        elif local_timestamp < s3_timestamp:
            logger.info('Need to download (UPDATE): %s', key)
            with open(local_path, 'wb') as fp:
                sync_client.put_file(key, fp)
        else:
            logger.info('No need to update: %s', key)

    sync_client.put_sync_index()


if __name__ == '__main__':
    main()
