# -*- coding: utf-8 -*-
import datetime
import json
import os

from botocore.exceptions import ClientError

from s3backup.clients import SyncClient, SyncObject


def to_timestamp(dt):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (dt - epoch) / datetime.timedelta(seconds=1)


class S3SyncClient(SyncClient):
    def __init__(self, client, bucket, prefix):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.index = self.load_index()

    def __repr__(self):
        return 'S3SyncClient<{}, {}>'.format(self.bucket, self.prefix)

    def index_path(self):
        return os.path.join(self.prefix, '.index')

    def put(self, key, sync_object):
        self.client.put_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
            Body=sync_object.fp,
        )
        self.set_remote_timestamp(key, sync_object.timestamp)

    def get(self, key):
        resp = self.client.get_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
        )
        return SyncObject(resp['Body'], to_timestamp(resp['LastModified']))

    def delete(self, key):
        resp = self.client.delete_objects(
            Bucket=self.bucket,
            Delete={
                'Objects': [{'Key': os.path.join(self.prefix, key)}]
            }
        )

        if 'Deleted' not in resp:
            raise IndexError('The specified key does not exist: {}'.format(key))

    def load_index(self):
        try:
            resp = self.client.get_object(
                Bucket=self.bucket,
                Key=self.index_path(),
            )
            data = json.loads(resp['Body'].read().decode('utf-8'))
            return data
        except (ClientError, ValueError):
            return {}

    def update_index(self):
        keys = self.get_all_keys()
        index = {}
        for key in keys:
            index[key] = {
                'remote_timestamp': self.get_remote_timestamp(key),
                'local_timestamp': self.get_real_local_timestamp(key),
            }

        self.client.put_object(
            Bucket=self.bucket,
            Key=self.index_path(),
            Body=json.dumps(index),
        )
        self.index = index

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
            if key == '.index':
                continue
            results.append(key)

        return results

    def get_real_local_timestamp(self, key):
        response = self.client.head_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
        )
        return to_timestamp(response['LastModified'])

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
