# -*- coding: utf-8 -*-

import json
import logging
import os

import boto3


from local_sync_client import LocalSyncClient
from s3_sync_client import S3SyncClient


logger = logging.getLogger(__name__)


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
        logger.info('Syncing "%s" with "%s"', local_dir, s3_key)

        local_client = LocalSyncClient(local_dir)
        s3_client = S3SyncClient(client, bucket, s3_key)

        perform_sync(s3_client, local_client)


def perform_sync(s3_client, local_client):
    keys1 = local_client.keys()
    keys2 = s3_client.keys()
    all_keys = set(keys1).union(set(keys2))

    for key in all_keys:
        s3_timestamp = s3_client.get_object_metadata(key)['timestamp']
        local_timestamp = local_client.get_object_timestamp(key)

        if s3_timestamp is None:
            logger.info('Need to upload (CREATE): %s', key)
            md5 = local_client.get_object_md5(key)
            fp = local_client.get_object(key)
            s3_client.put_object(key, fp, {
                'timestamp': local_timestamp,
                'md5': md5,
            })
            fp.close()
        elif local_timestamp is None:
            logger.info('Need to download (CREATE): %s', key)
            fp = s3_client.get_object(key)
            local_client.put_object(key, fp, s3_timestamp)
            fp.close()
        elif local_timestamp > s3_timestamp:
            logger.info('Need to upload (UPDATE): %s', key)
            md5 = local_client.get_object_md5(key)
            fp = local_client.get_object(key)
            s3_client.put_object(key, fp, {
                'timestamp': local_timestamp,
                'md5': md5,
            })
            fp.close()
        elif local_timestamp < s3_timestamp:
            logger.info('Need to download (UPDATE): %s', key)
            fp = s3_client.get_object(key)
            local_client.put_object(key, fp, s3_timestamp)
            fp.close()
        else:
            logger.debug('No need to update: %s', key)

    s3_client.update_sync_index()
    local_client.update_sync_index()


if __name__ == '__main__':
    sync()
