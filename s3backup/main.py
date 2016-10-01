# -*- coding: utf-8 -*-

import json
import logging
import os

import boto3
import tqdm

from s3backup.local_sync_client import LocalSyncClient
from s3backup.s3_sync_client import S3SyncClient

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


def get_progress_bar(max_value, desc):
    return tqdm.tqdm(
        total=max_value,
        leave=False,
        desc=desc,
        unit='B',
        unit_scale=True,
        mininterval=0.2,
    )


def perform_sync(s3_client, local_client):
    for operation, mode, key, timestamp, md5 in get_sync_status(s3_client, local_client):

        if operation == 'UPLOAD':
            logger.info('Need to upload (%s): %s', key, mode)
            total, fp, _ = local_client.get_object(key)

            with get_progress_bar(total, 'Uploading') as progressbar:
                s3_client.put_object(key, fp, {
                    'timestamp': timestamp,
                    'md5': md5,
                }, callback=progressbar.update)
                fp.close()

        elif operation == 'DOWNLOAD':
            logger.info('Need to download (%s): %s', mode, key)
            total, fp, _ = s3_client.get_object(key)

            with get_progress_bar(total, 'Downloading') as progressbar:
                local_client.put_object(
                    key, fp, timestamp,
                    callback=progressbar.update,
                )
                fp.close()

        elif operation is None:
            logger.debug('No need to update: %s', key)

    s3_client.update_sync_index()
    local_client.update_sync_index()


def get_sync_status(s3_client, local_client):
    keys1 = local_client.keys()
    keys2 = s3_client.keys()
    all_keys = set(keys1).union(set(keys2))

    for key in sorted(all_keys):
        s3_timestamp = s3_client.get_object_timestamp(key)
        local_timestamp = local_client.get_object_timestamp(key)

        if s3_timestamp is None:
            yield ('UPLOAD', 'CREATE', key, local_timestamp, None)

        elif local_timestamp is None:
            yield ('DOWNLOAD', 'CREATE', key, s3_timestamp, None)

        elif local_timestamp > s3_timestamp:
            local_md5 = local_client.get_object_md5(key)
            s3_md5 = s3_client.get_object_md5(key)
            if local_md5 != s3_md5:
                yield ('UPLOAD', 'UPDATE', key, local_timestamp, local_md5)

        elif local_timestamp < s3_timestamp:
            local_md5 = local_client.get_object_md5(key)
            s3_md5 = s3_client.get_object_md5(key)
            if local_md5 != s3_md5:
                yield ('DOWNLOAD', 'UPDATE', key, s3_timestamp, s3_md5)

        else:
            yield (None, None, key, None, None)


if __name__ == '__main__':
    sync()
