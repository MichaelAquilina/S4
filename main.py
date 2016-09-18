# -*- coding: utf-8 -*-

import json
import os

import botocore
import boto3

import logging


logger = logging.getLogger(__name__)


class SyncClient(object):
    def keys(self):
        raise NotImplemented()

    def put_object(self, key, fp, timestamp):
        raise NotImplemented()

    def get_object(self, key):
        raise NotImplemented()


class LocalSyncClient(object):
    def __init__(self, local_dir):
        self.local_dir = local_dir
        self.get_sync_index()

    def get_sync_index(self):
        try:
            sync_index_path = os.path.join(self.local_dir, '.syncindex')
            with open(sync_index_path, 'r') as fp:
                self.sync_index = json.load(fp)
        except FileNotFoundError:
            self.sync_index = {}

    def put_sync_index(self):
        sync_index_path = os.path.join(self.local_dir, '.syncindex')
        with open(sync_index_path, 'w') as fp:
            json.dump(self.sync_index, fp)

    def keys(self):
        return set(self.sync_index.keys())

    def put_object(self, key, fp, timestamp):
        self.sync_index[key] = timestamp
        key_path = os.path.join(self.local_dir, key)
        with open(key_path, 'wb') as fp2:
            fp2.write(fp.read())

    def get_object(self, key):
        return open(os.path.join(self.local_dir, key))


class S3SyncClient(object):
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
            logging.warning("Sync Index not found. Creating empty index")
            self.sync_index = {}

    def keys(self):
        return set(self.sync_index.keys())

    def put_sync_index(self):
        self.client.put_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, '.syncindex'),
            Body=json.dumps(self.sync_index),
        )

    def put_object(self, key, fp, timestamp):
        self.sync_index[key] = timestamp
        self.client.put_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
            Body=fp,
        )

    def get_object(self, key):
        return self.client.get_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
        )['Body']


def sync():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--loglevel', default=logging.INFO)

    args = parser.parse_args()

    logger.addHandler(logging.StreamHandler())
    logger.setLevel(args.loglevel)

    client = boto3.client('s3')

    with open(os.path.expanduser('~/.s3syncrc'), 'r') as fp:
        configuration = json.load(fp)

    bucket = configuration['bucket']
    directories = configuration['directories']

    for local_dir, s3_key in directories.items():
        logger.info("Syncing %s with %s", local_dir, s3_key)

        local_client = LocalSyncClient(local_dir)
        s3_client = S3SyncClient(client, bucket, s3_key)

        perform_sync(s3_client, local_client)


def perform_sync(s3_client, local_client):
    all_keys = local_client.keys().union(s3_client.keys())
    for key in all_keys:
        s3_timestamp = s3_client.sync_index.get(key)
        local_timestamp = local_client.sync_index.get(key)

        if s3_timestamp is None:
            logger.info('Need to upload (CREATE): %s', key)
            fp = local_client.get_object(key)
            s3_client.put_file(key, fp, local_timestamp)
            fp.close()
        elif local_timestamp is None:
            logger.info('Need to download (CREATE): %s', key)
            fp = s3_client.get_object(key)
            local_client.put_object(key, fp, s3_timestamp)
            fp.close()
        elif local_timestamp > s3_timestamp:
            logger.info('Need to upload (UPDATE): %s', key)
            fp = local_client.get_object(key)
            s3_client.put_file(key, fp, local_timestamp)
            fp.close()
        elif local_timestamp < s3_timestamp:
            logger.info('Need to download (UPDATE): %s', key)
            fp = s3_client.get_object(key)
            local_client.put_object(key, fp, s3_timestamp)
            fp.close()
        else:
            logger.info('No need to update: %s', key)

    s3_client.put_sync_index()
    local_client.put_sync_index()

    assert s3_client.sync_index == local_client.sync_index


if __name__ == '__main__':
    sync()
