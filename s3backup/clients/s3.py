# -*- coding: utf-8 -*-
import collections
import datetime
import fnmatch
import json
import logging
import os
import zlib

from botocore.exceptions import ClientError

import magic

from s3backup.clients import SyncClient, SyncObject, SyncState


logger = logging.getLogger(__name__)


S3Uri = collections.namedtuple('S3Uri', ['bucket', 'key'])


def parse_s3_uri(uri):
    tokens = uri.replace('s3://', '').split('/')
    if len(tokens) < 2:
        return None

    bucket = tokens[0]
    key = '/'.join(tokens[1:])
    return S3Uri(bucket, key)


def to_timestamp(dt):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (dt - epoch) / datetime.timedelta(seconds=1)


class S3SyncClient(SyncClient):
    def __init__(self, client, bucket, prefix, ignore_files=None):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.index = self.load_index()
        self.ignore_files = ['.index']
        if ignore_files is not None:
            self.ignore_files.extend(ignore_files)

    def __repr__(self):
        return 'S3SyncClient<{}, {}>'.format(self.bucket, self.prefix)

    def get_uri(self):
        return 's3://{}/{}'.format(self.bucket, self.prefix)

    def index_path(self):
        return os.path.join(self.prefix, '.index')

    def put(self, key, sync_object, callback=None):
        self.client.upload_fileobj(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
            Fileobj=sync_object.fp,
            Callback=callback,
        )
        self.set_remote_timestamp(key, sync_object.timestamp)

    def get(self, key):
        resp = self.client.get_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
        )
        return SyncObject(resp['Body'], resp['ContentLength'], to_timestamp(resp['LastModified']))

    def delete(self, key):
        resp = self.client.delete_objects(
            Bucket=self.bucket,
            Delete={
                'Objects': [{'Key': os.path.join(self.prefix, key)}]
            }
        )
        return 'Deleted' in resp

    def load_index(self):
        try:
            resp = self.client.get_object(
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

    def flush_index(self, compressed=True):
        data = json.dumps(self.index).encode('utf-8')
        if compressed:
            logger.debug('Using zlib encoding for writing index')
            data = zlib.compress(data)
        else:
            logger.debug('Using plain text encoding for writing index')

        self.client.put_object(
            Bucket=self.bucket,
            Key=self.index_path(),
            Body=data,
        )

    def get_local_keys(self):
        results = []
        resp = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=self.prefix,
        )
        if 'Contents' not in resp:
            return results

        for obj in resp['Contents']:
            key = os.path.relpath(obj['Key'], self.prefix)
            if not any(fnmatch.fnmatch(key, pattern) for pattern in self.ignore_files):
                results.append(key)
            else:
                logger.debug('Ignoring %s', key)

        return results

    def get_real_local_timestamp(self, key):
        try:
            response = self.client.head_object(
                Bucket=self.bucket,
                Key=os.path.join(self.prefix, key),
            )
            return to_timestamp(response['LastModified'])
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

    def get_actions(self, keys):
        # overwrite default for better performance
        real_local_timestamps = {}
        resp = self.client.list_objects_v2(
            Bucket=self.bucket,
            Prefix=self.prefix,
        )
        for obj in resp.get('Contents', []):
            key = os.path.relpath(obj['Key'], self.prefix)
            real_local_timestamps[key] = to_timestamp(obj['LastModified'])

        results = {}
        for key in keys:
            index_local_timestamp = self.get_index_local_timestamp(key)
            real_local_timestamp = real_local_timestamps.get(key)
            remote_timestamp = self.get_remote_timestamp(key)

            if index_local_timestamp is None and real_local_timestamp:
                results[key] = SyncState(SyncState.UPDATED, real_local_timestamp)
            elif real_local_timestamp is None and index_local_timestamp:
                results[key] = SyncState(SyncState.DELETED, remote_timestamp)
            elif real_local_timestamp is None and index_local_timestamp is None and remote_timestamp:
                results[key] = SyncState(SyncState.DELETED, remote_timestamp)
            elif real_local_timestamp is None and index_local_timestamp is None:
                # Does not exist in this case, so no action to perform
                results[key] = SyncState(SyncState.DOESNOTEXIST, None)
            elif index_local_timestamp < int(real_local_timestamp):
                results[key] = SyncState(SyncState.UPDATED, real_local_timestamp)
            elif index_local_timestamp > int(real_local_timestamp):
                results[key] = SyncState(SyncState.CONFLICT, index_local_timestamp)   # corruption?
            else:
                results[key] = SyncState(SyncState.NOCHANGES, remote_timestamp)

        return results
