# -*- coding: utf-8 -*-

import gzip
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

    def get_object_timestamp(self, key):
        object_path = os.path.join(self.local_dir, key)
        if os.path.exists(object_path):
            return os.stat(object_path).st_mtime
        else:
            return None

    def update_sync_index(self):
        pass

    def keys(self):
        for item in os.listdir(self.local_dir):
            if item.startswith('.'):
                continue

            if os.path.isfile(os.path.join(self.local_dir, item)):
                yield item

    def put_object(self, key, fp, timestamp):
        object_path = os.path.join(self.local_dir, key)
        object_stat = os.stat(object_path)
        with open(object_path, 'wb') as fp2:
            fp2.write(fp.read())
        os.utime(object_path, (object_stat.st_atime, timestamp))

    def get_object(self, key):
        return open(os.path.join(self.local_dir, key), 'rb')


class S3SyncClient(object):
    SYNC_INDEX = '.syncindex.json.gz'

    def __init__(self, client, bucket, prefix):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self._get_sync_index()

    def _get_sync_index(self):
        logger.info('Getting sync index from %s', self.bucket)
        try:
            data = self.client.get_object(
                Bucket=self.bucket,
                Key=os.path.join(self.prefix, self.SYNC_INDEX),
            )['Body']
            json_data = gzip.decompress(data.read()).decode('utf-8')
            self.sync_index = json.loads(json_data)
        except botocore.exceptions.ClientError:
            logger.warning("Sync Index not found. Creating empty index")
            self.sync_index = {}
        finally:
            self._sync_index_dirty = False

    def get_object_timestamp(self, key):
        return self.sync_index.get(key)

    def set_object_timestamp(self, key, timestamp):
        self.sync_index[key] = timestamp
        self._sync_index_dirty = True

    def keys(self):
        return self.sync_index.keys()

    def update_sync_index(self):
        if self._sync_index_dirty:
            sync_data = json.dumps(self.sync_index).encode('utf-8')
            self.client.put_object(
                Bucket=self.bucket,
                Key=os.path.join(self.prefix, self.SYNC_INDEX),
                Body=gzip.compress(sync_data),
            )
            self._sync_index_dirty = False

    def put_object(self, key, fp, timestamp):
        self.client.put_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
            Body=fp,
        )
        self.set_object_timestamp(key, timestamp)

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
    keys1 = local_client.keys()
    keys2 = s3_client.keys()
    all_keys = set(keys1).union(set(keys2))

    for key in all_keys:
        s3_timestamp = s3_client.get_object_timestamp(key)
        local_timestamp = local_client.get_object_timestamp(key)

        if s3_timestamp is None:
            logger.info('Need to upload (CREATE): %s', key)
            fp = local_client.get_object(key)
            s3_client.put_object(key, fp, local_timestamp)
            fp.close()
        elif local_timestamp is None:
            logger.info('Need to download (CREATE): %s', key)
            fp = s3_client.get_object(key)
            local_client.put_object(key, fp, s3_timestamp)
            fp.close()
        elif local_timestamp > s3_timestamp:
            logger.info('Need to upload (UPDATE): %s', key)
            fp = local_client.get_object(key)
            s3_client.put_object(key, fp, local_timestamp)
            fp.close()
        elif local_timestamp < s3_timestamp:
            logger.info('Need to download (UPDATE): %s', key)
            fp = s3_client.get_object(key)
            local_client.put_object(key, fp, s3_timestamp)
            fp.close()
        else:
            logger.info('No need to update: %s', key)

    s3_client.update_sync_index()


if __name__ == '__main__':
    sync()
