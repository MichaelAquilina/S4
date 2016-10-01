# -*- coding: utf-8 -*-

import datetime as dt
import gzip
import hashlib
import io
import json

import boto3
import freezegun
import pytz
import moto

from s3backup.s3_sync_client import S3SyncClient, generate_index


class TestGenerateIndex(object):
    @moto.mock_s3
    def test_empty_bucket(self):
        client = boto3.client('s3')
        client.create_bucket(Bucket='testbucket')
        assert generate_index(client, 'testbucket', 'foobar') == {}

    @moto.mock_s3
    def test_correct_output(self):
        client = boto3.client('s3')
        client.create_bucket(Bucket='testbucket')

        body = b'ahoy there matey!'
        md5 = hashlib.md5()
        md5.update(body)

        with freezegun.freeze_time('2016-09-30 13:30'):
            client.put_object(
                Bucket='testbucket',
                Key='piratestreasure/xmarksthespot.txt',
                Body=io.BytesIO(b'I should not be part of the output'),
            )
            client.put_object(
                Bucket='testbucket',
                Key='pirates/hello.txt',
                Body=io.BytesIO(body),
            )

        actual_index = generate_index(client, 'testbucket', 'pirates')
        expected_index = {
            'hello.txt': {
                'timestamp': None,
                'LastModified': dt.datetime(2016, 9, 30, 13, 30, tzinfo=pytz.UTC),
                'size': len(body),
                'md5': md5.hexdigest(),
            }
        }

        assert actual_index == expected_index


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

        sync_client.put_object('dead/pool', io.BytesIO(b'testing'), {'timestamp': 20000})
        assert sync_client.sync_index == {
            'dead/pool': {'timestamp': 20000},
        }

    @moto.mock_s3
    def test_existing_sync_index(self):
        sync_index = {
            'foo': {'timestamp': 123213213, 'LastModified': 423232},
            'bar': {'timestamp': 231412323, 'LastModified': 324232},
        }
        sync_client = setup_sync_client(sync_index=sync_index)
        assert sync_client.sync_index == sync_index

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
        md5 = hashlib.md5()
        md5.update(content)
        md5_hash = md5.hexdigest()

        target_object = io.BytesIO(content)
        sync_client = setup_sync_client()
        sync_client.put_object(key, target_object, {
            'timestamp': timestamp,
            'md5': md5_hash,
        })

        _, data, metadata = sync_client.get_object(key)
        index_metadata = sync_client.get_object_metadata(key)

        assert index_metadata['timestamp'] == timestamp == float(metadata['timestamp'])
        assert index_metadata['md5'] == md5_hash == metadata['md5']

        assert data.read() == content

        assert sync_client._dirty_keys == {'apples/oranges.txt'}

    @moto.mock_s3
    def test_get_object_metadata(self):
        sync_index = {
            'foo': {'timestamp': 123213213, 'LastModified': 423230},
            'bar': {'timestamp': 231412323, 'LastModified': 324232},
            'car': {'LastModified': 42323},
        }
        sync_client = setup_sync_client(sync_index=sync_index)
        assert sync_client.get_object_metadata('foo') == {
            'timestamp': 123213213,
            'LastModified': 423230,
        }
        assert sync_client.get_object_metadata('bar') == {
            'timestamp': 231412323,
            'LastModified': 324232,
        }
        assert sync_client.get_object_metadata('car') == {
            'LastModified': 42323,
        }
        assert sync_client.get_object_metadata('idontexist') is None

    @moto.mock_s3
    def test_set_object_metadata(self):
        sync_index = {
            'blargh': {'timestamp': 99999999, 'LastModified': 9999999},
        }
        sync_client = setup_sync_client(sync_index=sync_index)
        sync_client.set_object_metadata('blargh', {'timestamp': 11111111})
        # TODO: should probably *not* work
        sync_client.set_object_metadata('idontexist', {'timestamp': 2323232})

        assert sync_client.get_object_metadata('blargh') == {
            'timestamp': 11111111,
            'LastModified': 9999999,
        }
        assert sync_client.get_object_metadata('idontexist') == {
            'timestamp': 2323232,
        }

        assert sync_client._dirty_keys == {'blargh', 'idontexist'}

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

        sync_client.put_object('hello/world', io.BytesIO(b'hello'), {'timestamp': 20000000})
        assert sync_client.sync_index == {
            'hello/world': {'timestamp': 20000000}
        }
        assert sync_client._dirty_keys == {'hello/world'}

        sync_client.update_sync_index()

        assert sync_client._dirty_keys == set()

        result = client.get_object(
            Bucket=bucket,
            Key='{}/.syncindex.json.gz'.format(prefix),
        )

        data = gzip.decompress(result['Body'].read())
        stored_index = json.loads(data.decode('utf-8'))
        assert set(stored_index.keys()) == {'hello/world'}
        assert stored_index['hello/world']['timestamp'] == 20000000
        assert stored_index['hello/world']['LastModified'] is not None
