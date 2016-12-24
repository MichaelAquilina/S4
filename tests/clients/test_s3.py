# -*- coding: utf-8 -*-
import datetime
import io
import json

import boto3
from botocore.exceptions import ClientError

import freezegun

from moto import mock_s3

import pytest

from s3backup.clients import s3


def touch(client, bucket, key, timestamp):
    last_modified = datetime.datetime.utcfromtimestamp(timestamp)
    with freezegun.freeze_time(last_modified):
        client.put_object(Bucket=bucket, Key=key)


class TestS3SyncClient(object):
    def setup_method(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )

    @mock_s3
    def test_put(self):
        self.s3_client.create_bucket(Bucket='testbucket')
        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        fp = io.BytesIO(b'hello world')
        client.put('one/night/ultimate', fp, 10000)

        assert client.index['one/night/ultimate'] == {
            'remote_timestamp': 10000
        }
        resp = self.s3_client.get_object(
            Bucket='testbucket',
            Key='Foo/one/night/ultimate',
        )
        assert resp['Body'].read() == b'hello world'

    @mock_s3
    def test_get(self):
        self.s3_client.create_bucket(Bucket='testbucket')
        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        self.s3_client.put_object(
            Bucket='testbucket',
            Key='Foo/red/giant',
            Body=b'what is up?',
        )
        fp = client.get('red/giant')
        assert fp.read() == b'what is up?'

    @mock_s3
    def test_delete(self):
        self.s3_client.create_bucket(Bucket='testbucket')
        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        self.s3_client.put_object(
            Bucket='testbucket',
            Key='Foo/bar/baz',
            Body=b'aaaaa',
        )

        client.delete('bar/baz')
        with pytest.raises(ClientError):
            self.s3_client.get_object(
                Bucket='testbucket',
                Key='Foo/bar/baz',
            )

    @mock_s3
    def test_index_path(self):
        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        assert client.index_path() == 'Foo/.index'

    @mock_s3
    def test_get_index_state(self):
        data = {
            'red': {'remote_timestamp': 23132},
            'green': {'remote_timestamp': 4000},
            'blue': {'remote_timestamp': 9000},
        }
        self.s3_client.create_bucket(Bucket='testbucket')
        self.s3_client.put_object(
            Bucket='testbucket',
            Key='Foo/.index',
            Body=json.dumps(data),
        )

        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        actual_output = client.get_index_state()
        assert actual_output == data

    @mock_s3
    def test_get_current_state(self):
        self.s3_client.create_bucket(Bucket='testbucket')

        touch(self.s3_client, 'testbucket', 'Foo/.index', 432300)
        touch(self.s3_client, 'testbucket', 'Foo/Bar', 40000)
        touch(self.s3_client, 'testbucket', 'Foo/Baz/Car.txt', 123456)

        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        actual_output = client.get_current_state()
        expected_output = {
            'Bar': {'local_timestamp': 40000},
            'Baz/Car.txt': {'local_timestamp': 123456},
        }
        assert actual_output == expected_output

    @mock_s3
    def test_update_index_empty(self):
        self.s3_client.create_bucket(Bucket='testbucket')

        touch(self.s3_client, 'testbucket', 'Foo/Bar', 40000)
        touch(self.s3_client, 'testbucket', 'Foo/Baz/Car.txt', 123456)

        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        client.update_index()
        actual_output = client.get_index_state()
        expected_output = {
            'Bar': {'local_timestamp': 40000},
            'Baz/Car.txt': {'local_timestamp': 123456},
        }
        assert actual_output == expected_output

    @mock_s3
    def test_update_index_non_empty(self):
        self.s3_client.create_bucket(Bucket='testbucket')

        touch(self.s3_client, 'testbucket', 'Foo/Bar', 40000)
        touch(self.s3_client, 'testbucket', 'Foo/Baz/Car.txt', 123456)

        self.s3_client.put_object(
            Bucket='testbucket',
            Key='Foo/.index',
            Body=json.dumps({
                'Bar': {
                    'remote_timestamp': 13333333,
                    'local_timestamp': 90000000,
                },
                'Baz/Car.txt': {
                    'remote_timestamp': 30000000,
                    'local_timestamp': 4000000,
                },
                'DeleteMe': {
                    'remote_timestamp': 33333333,
                    'local_timestamp': 32323332,
                }
            })
        )

        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        last_modified = datetime.datetime.utcfromtimestamp(888888888)
        with freezegun.freeze_time(last_modified):
            client.put('Baz/Car.txt', io.BytesIO(b''), 1234567890)

        client.update_index()
        actual_output = client.get_index_state()
        expected_output = {
            'Bar': {
                'remote_timestamp': 13333333,
                'local_timestamp': 40000
            },
            'Baz/Car.txt': {
                'remote_timestamp': 1234567890,
                'local_timestamp': 888888888,
            },
        }
        assert actual_output == expected_output
