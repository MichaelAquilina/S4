# -*- coding: utf-8 -*-

import gzip
import json
from io import BytesIO

import boto3
import moto

from s3backup.s3_sync_client import S3SyncClient


def setup_sync_client(client=None, bucket='testbucket', key='Music', sync_index={}):
    if client is None:
        client = boto3.client('s3')

    client.create_bucket(Bucket=bucket)

    client.put_object(
        Bucket=bucket,
        Key='{}/.syncindex.json.gz'.format(key),
        Body=gzip.compress(json.dumps(sync_index).encode('utf-8'))
    )

    return S3SyncClient(client, bucket, key)


class TestS3SyncClient(object):

    @moto.mock_s3
    def test_no_sync_index(self):
        client = boto3.client('s3')
        client.create_bucket(Bucket='testbucket')

        sync_client = S3SyncClient(client, 'testbucket', 'Music/')
        assert sync_client.sync_index == {}

    @moto.mock_s3
    def test_existing_sync_index(self):
        sync_index = {
            'foo': {'timestamp': 123213213, 'LastModified': 423232},
            'bar': {'timestamp': 231412323, 'LastModified': 324232},
        }
        sync_client = setup_sync_client(sync_index=sync_index)
        assert sync_client.sync_index == sync_index

    @moto.mock_s3
    def test_mends_old_indexes(self):
        # TODO: Can eventually remove this
        sync_index = {
            'foo': 123213213,
            'bar': 231412323,
        }
        sync_client = setup_sync_client(sync_index=sync_index)
        assert sync_client.sync_index == {
            'foo': {'timestamp': 123213213, 'LastModified': None},
            'bar': {'timestamp': 231412323, 'LastModified': None},
        }

    @moto.mock_s3
    def test_keys(self):
        sync_index = {
            'A': {'timestamp': 111111, 'LastModified': 232414},
            'B': {'timestamp': 111111, 'LastModified': 232414},
            'C': {'timestamp': 111111, 'LastModified': 232414},
            'E': {'timestamp': 111111, 'LastModified': 232414},
        }
        sync_client = setup_sync_client(sync_index=sync_index)
        assert set(sync_client.keys()) == {'A', 'B', 'C', 'E'}

    @moto.mock_s3
    def test_put_get_object(self):
        key = 'apples/oranges.txt'
        timestamp = 13371337
        content = b'hello world'

        target_object = BytesIO(content)
        sync_client = setup_sync_client()
        sync_client.put_object(key, target_object, timestamp)

        assert sync_client.get_object_timestamp(key) == timestamp
        assert sync_client.get_object(key).read() == content

    @moto.mock_s3
    def test_get_object_timestamp(self):
        sync_index = {
            'foo': {'timestamp': 123213213, 'LastModified': 423232},
            'bar': {'timestamp': 231412323, 'LastModified': 324232},
        }
        sync_client = setup_sync_client(sync_index=sync_index)
        assert sync_client.get_object_timestamp('foo') == 123213213
        assert sync_client.get_object_timestamp('bar') == 231412323
        assert sync_client.get_object_timestamp('idontexist') is None

    @moto.mock_s3
    def test_set_object_timestamp(self):
        sync_index = {
            'blargh': {'timestamp': 99999999, 'LastModified': 9999999},
        }
        sync_client = setup_sync_client(sync_index=sync_index)
        sync_client.set_object_timestamp('blargh', 11111111)
        # TODO: should probably *not* work
        sync_client.set_object_timestamp('idontexist', 2323232)

        assert sync_client.get_object_timestamp('blargh') == 11111111
        assert sync_client.get_object_timestamp('idontexist') == 2323232

    @moto.mock_s3
    def test_update_sync_index(self):
        bucket = 'fuzzywuzzybucket'
        prefix = 'Poll'

        client = boto3.client('s3')
        sync_client = setup_sync_client(
            client=client,
            bucket=bucket,
            key=prefix,
        )
        assert sync_client.sync_index == {}

        sync_client.put_object('hello/world', BytesIO(b'hello'), 20000000)
        assert sync_client.sync_index == {
            'hello/world': {'timestamp': 20000000, 'LastModified': None}
        }
        sync_client.update_sync_index()
        result = client.get_object(
            Bucket=bucket,
            Key='{}/.syncindex.json.gz'.format(prefix),
        )

        data = gzip.decompress(result['Body'].read())
        stored_index = json.loads(data.decode('utf-8'))
        assert set(stored_index.keys()) == {'hello/world'}
        assert stored_index['hello/world']['timestamp'] == 20000000
        assert stored_index['hello/world']['LastModified'] is not None
