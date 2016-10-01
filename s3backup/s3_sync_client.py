# -*- coding: utf-8 -*-

import gzip
import json
import logging
import os

import botocore

logger = logging.getLogger(__name__)


def generate_index(client, bucket, prefix):
    if not prefix.endswith('/'):
        prefix += '/'

    results = {}
    response = client.list_objects(
        Bucket=bucket,
        Prefix=prefix,
    )
    if 'Contents' not in response:
        return {}

    for obj in response['Contents']:
        key = obj['Key'].replace(prefix, '', 1)
        results[key] = {
            'timestamp': None,
            'LastModified': obj['LastModified'],
            'size': obj['Size'],
            'md5': json.loads(obj['ETag']),
        }
    return results


class S3SyncClient(object):
    SYNC_INDEX = '.syncindex.json.gz'

    def __init__(self, client, bucket, prefix):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self._get_sync_index()

    @property
    def sync_index_path(self):
        return os.path.join(self.prefix, self.SYNC_INDEX)

    def _get_sync_index(self):
        logger.info('Getting sync index from %s', self.bucket)
        try:
            data = self.client.get_object(
                Bucket=self.bucket,
                Key=self.sync_index_path,
            )['Body']
            json_data = gzip.decompress(data.read()).decode('utf-8')
            self.sync_index = json.loads(json_data)
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.warning("Sync Index not found. Recreating")
                self.sync_index = {}
            else:
                raise
        finally:
            self._dirty_keys = set()

    def get_object_metadata(self, key, metadata=None):
        return self.sync_index.get(key)

    def set_object_metadata(self, key, metadata):
        if key not in self.sync_index:
            self.sync_index[key] = {}
        self.sync_index[key].update(metadata)
        self._dirty_keys.add(key)

    def get_object_timestamp(self, key):
        metadata = self.get_object_metadata(key)
        if metadata:
            return metadata.get('timestamp')

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
                Key=self.sync_index_path,
                Body=gzip.compress(sync_data),
            )
            self._dirty_keys.clear()

    def put_object(self, key, fp, metadata, callback=None):
        self.client.upload_fileobj(
            Fileobj=fp,
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
            ExtraArgs={'Metadata': {k: str(v) for k, v in metadata.items()}},
            Callback=callback,
        )
        self.set_object_metadata(key, metadata)

    def get_object_md5(self, key):
        return self.client.head_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
        )['ETag'].strip('"')

    def get_object(self, key):
        response = self.client.get_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
        )
        return (
            response['ContentLength'],
            response['Body'],
            response['Metadata'],
        )
