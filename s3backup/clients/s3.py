# -*- coding: utf-8 -*-
import datetime
import json
import os

from botocore.exceptions import ClientError


def to_timestamp(dt):
    epoch = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    return (dt - epoch) / datetime.timedelta(seconds=1)


class S3SyncClient(object):

    def __init__(self, client, bucket, prefix):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix
        self.index = self.get_index_state()

    def index_path(self):
        return os.path.join(self.prefix, '.index')

    def get_absolute_path(self, path):
        return os.path.join(self.prefix, path)

    def put(self, key, fp, remote_timestamp):
        self.client.put_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
            Body=fp,
        )
        if key not in self.index:
            self.index[key] = {}
        self.index[key]['remote_timestamp'] = remote_timestamp

    def get(self, key):
        resp = self.client.get_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
        )
        return resp['Body']

    def delete(self, key):
        self.client.delete_object(
            Bucket=self.bucket,
            Key=os.path.join(self.prefix, key),
        )

    def get_index_state(self):
        try:
            resp = self.client.get_object(
                Bucket=self.bucket,
                Key=self.index_path(),
            )
            data = json.loads(resp['Body'].read().decode('utf-8'))
            return data
        except (ClientError, ValueError):
            return {}

    def get_current_state(self):
        results = {}
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

            results[key] = {
                'local_timestamp': to_timestamp(obj['LastModified']),
            }
        return results

    def update_index(self):
        results = self.get_current_state()
        for key in results:
            if key in self.index:
                results[key]['remote_timestamp'] = self.index[key]['remote_timestamp']

        self.client.put_object(
            Bucket=self.bucket,
            Key=self.index_path(),
            Body=json.dumps(results),
        )
