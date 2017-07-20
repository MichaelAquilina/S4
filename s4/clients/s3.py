# -*- coding: utf-8 -*-
import collections
import fnmatch
import json
import logging
import os
import zlib

from botocore.exceptions import ClientError

import magic

from s4 import utils
from s4.clients import SyncClient, SyncObject


logger = logging.getLogger(__name__)


S3Uri = collections.namedtuple('S3Uri', ['bucket', 'key'])


def parse_s3_uri(uri):
    if not uri.startswith('s3://'):
        return None

    tokens = uri.replace('s3://', '').split('/')
    if len(tokens) < 2:
        return None

    bucket = tokens[0]
    key = '/'.join(tokens[1:])
    return S3Uri(bucket, key)


def is_ignored_key(key, ignore_files):
    # Check if any subdirectories match the ignore patterns
    key_parts = key.split('/')
    for part in key_parts:
        if any(fnmatch.fnmatch(part, pattern) for pattern in ignore_files):
            return True
    else:
        return False


class S3SyncClient(SyncClient):
    def __init__(self, boto, bucket, prefix):
        self.boto = boto
        self.bucket = bucket
        self.prefix = prefix
        self.index = self.load_index()
        self.reload_ignore_files()

    def get_client_name(self):
        return 's3'

    def __repr__(self):
        return 'S3SyncClient<{}, {}>'.format(self.bucket, self.prefix)

    def get_uri(self, key=''):
        return 's3://{}/{}'.format(self.bucket, os.path.join(self.prefix, key))

    def index_path(self):
        return os.path.join(self.prefix, '.index')

    def put(self, key, sync_object, callback=None):
        self.boto.upload_fileobj(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
            Fileobj=sync_object.fp,
            Callback=callback,
        )
        self.set_remote_timestamp(key, sync_object.timestamp)

    def get(self, key):
        try:
            resp = self.boto.get_object(
                Bucket=self.bucket,
                Key=os.path.join(self.prefix, key),
            )
            return SyncObject(
                resp['Body'],
                resp['ContentLength'],
                utils.to_timestamp(resp['LastModified']),
            )
        except ClientError:
            return None

    def delete(self, key):
        resp = self.boto.delete_objects(
            Bucket=self.bucket,
            Delete={
                'Objects': [{'Key': os.path.join(self.prefix, key)}]
            }
        )
        return 'Deleted' in resp

    def load_index(self):
        try:
            resp = self.boto.get_object(
                Bucket=self.bucket,
                Key=self.index_path(),
            )
            body = resp['Body'].read()
            content_type = magic.from_buffer(body, mime=True)
            if content_type == 'text/plain':
                logger.debug('Detected plain text encoding for index')
                return json.loads(body.decode('utf-8'))
            elif content_type == 'application/zlib':
                logger.debug('Detected zlib encoding for index')
                body = zlib.decompress(body)
                return json.loads(body.decode('utf-8'))
            elif content_type == 'application/x-empty':
                return {}
            else:
                raise ValueError('Unknown content type for index', content_type)
        except (ClientError):
            return {}

    def reload_index(self):
        self.index = self.load_index()

    def flush_index(self, compressed=True):
        data = json.dumps(self.index).encode('utf-8')
        if compressed:
            logger.debug('Using zlib encoding for writing index')
            data = zlib.compress(data)
        else:
            logger.debug('Using plain text encoding for writing index')

        self.boto.put_object(
            Bucket=self.bucket,
            Key=self.index_path(),
            Body=data,
        )

    def get_local_keys(self):
        results = []
        paginator = self.boto.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=self.bucket,
            Prefix=self.prefix,
        )
        for page in page_iterator:
            if 'Contents' not in page:
                return results

            for obj in page['Contents']:
                key = os.path.relpath(obj['Key'], self.prefix)
                if not is_ignored_key(key, self.ignore_files):
                    results.append(key)
                else:
                    logger.debug('Ignoring %s', key)

        return results

    def get_real_local_timestamp(self, key):
        try:
            response = self.boto.head_object(
                Bucket=self.bucket,
                Key=os.path.join(self.prefix, key),
            )
            return utils.to_timestamp(response['LastModified'])
        except ClientError:
            return None

    def get_index_keys(self):
        return self.index.keys()

    def get_index_local_timestamp(self, key):
        return self.index.get(key, {}).get('local_timestamp')

    def set_index_local_timestamp(self, key, timestamp):
        if key not in self.index:
            self.index[key] = {}
        self.index[key]['local_timestamp'] = timestamp

    def get_remote_timestamp(self, key):
        return self.index.get(key, {}).get('remote_timestamp')

    def set_remote_timestamp(self, key, timestamp):
        if key not in self.index:
            self.index[key] = {}
        self.index[key]['remote_timestamp'] = timestamp

    def get_all_real_local_timestamps(self):
        result = {}
        paginator = self.boto.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=self.bucket,
            Prefix=self.prefix,
        )
        for page in page_iterator:
            for obj in page.get('Contents', []):
                key = os.path.relpath(obj['Key'], self.prefix)
                if not is_ignored_key(key, self.ignore_files):
                    result[key] = utils.to_timestamp(obj['LastModified'])

        return result

    def get_all_remote_timestamps(self):
        return {key: value.get('remote_timestamp') for key, value in self.index.items()}

    def get_all_index_local_timestamps(self):
        return {key: value.get('local_timestamp') for key, value in self.index.items()}

    def reload_ignore_files(self):
        self.ignore_files = ['.index']
        try:
            response = self.boto.get_object(
                Bucket=self.bucket,
                Key=os.path.join(self.prefix, '.syncignore')
            )
            data = response['Body'].read()
            data = data.decode('utf8')
            ignore_list = data.split('\n')
            self.ignore_files.extend(ignore_list)
        except ClientError:
            pass
