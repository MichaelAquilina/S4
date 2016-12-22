# -*- coding: utf-8 -*-
import datetime
import json

import boto3

import freezegun

from moto import mock_s3

from s3backup.clients import s3
from s3backup.clients.entries import FileEntry


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
    def test_index_path(self):
        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        assert client.index_path() == 'Foo/.index'

    @mock_s3
    def test_get_index_state(self):
        self.s3_client.create_bucket(Bucket='testbucket')
        self.s3_client.put_object(
            Bucket='testbucket',
            Key='Foo/.index',
            Body=json.dumps({
                'red': {'timestamp': 23132},
                'green': {'timestamp': 4000},
                'blue': {'timestamp': 9000},
            }),
        )

        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        actual_output = client.get_index_state()
        expected_output = {
            'red': FileEntry('red', 23132),
            'green': FileEntry('green', 4000),
            'blue': FileEntry('blue', 9000),
        }
        assert actual_output == expected_output

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
            'Bar': FileEntry('Bar', 40000),
            'Baz/Car.txt': FileEntry('Baz/Car.txt', 123456),
        }
        assert actual_output == expected_output

    @mock_s3
    def test_update_index(self):
        self.s3_client.create_bucket(Bucket='testbucket')

        touch(self.s3_client, 'testbucket', 'Foo/Bar', 40000)
        touch(self.s3_client, 'testbucket', 'Foo/Baz/Car.txt', 123456)

        client = s3.S3SyncClient(
            self.s3_client, 'testbucket', 'Foo'
        )
        client.update_index()
        assert client.get_index_state() == client.get_current_state()
