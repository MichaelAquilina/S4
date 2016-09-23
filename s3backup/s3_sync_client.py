# -*- coding: utf-8 -*-

import gzip
import json
import logging
import os

import botocore


logger = logging.getLogger(__name__)


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
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                # Should only set to default if the file is not found. Should check exception type
                logger.warning("Sync Index not found. Creating empty index")
                self.sync_index = {}
            else:
                raise
        else:
            self._dirty_keys = set()

    def get_object_timestamp(self, key):
        metadata = self.sync_index.get(key)
        if metadata:
            return metadata['timestamp']

    def set_object_timestamp(self, key, timestamp):
        if key not in self.sync_index:
            self.sync_index[key] = {'LastModified': None}
        self.sync_index[key]['timestamp'] = timestamp
        self._dirty_keys.add(key)

    def keys(self):
        return self.sync_index.keys()

    def update_sync_index(self):
        if len(self._dirty_keys) > 0:
            data = self.client.list_objects(Bucket=self.bucket, Prefix=self.prefix)
            for s3_object in data['Contents']:
                key = s3_object['Key'].replace(self.prefix, '', 1).lstrip('/')
                if key in self._dirty_keys and key not in (self.SYNC_INDEX, '.syncindex'):
                    timestamp = s3_object['LastModified'].timestamp()
                    self.sync_index[key]['LastModified'] = timestamp

            sync_data = json.dumps(self.sync_index).encode('utf-8')

            self.client.put_object(
                Bucket=self.bucket,
                Key=os.path.join(self.prefix, self.SYNC_INDEX),
                Body=gzip.compress(sync_data),
            )
            self._dirty_keys.clear()

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
