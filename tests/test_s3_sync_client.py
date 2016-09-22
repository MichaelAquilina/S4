# -*- coding: utf-8 -*-

import gzip
import json

import boto3
import moto

from s3backup.s3_sync_client import S3SyncClient


def set_index(client, bucket, key, data):
    client.put_object(
        Bucket='testbucket',
        Key='{}/.syncindex.json.gz'.format(key),
        Body=gzip.compress(json.dumps(data).encode('utf-8'))
    )


class TestS3SyncClient(object):

    @moto.mock_s3
    def test_no_sync_index(self):
        client = boto3.client('s3')
        client.create_bucket(Bucket='testbucket')

        sync_client = S3SyncClient(client, 'testbucket', 'Music/')
        assert sync_client.sync_index == {}

    @moto.mock_s3
    def test_existing_sync_index(self):
        client = boto3.client('s3')
        client.create_bucket(Bucket='testbucket')

        sync_index = {
            'foo': {'timestamp': 123213213, 'DateModified': 423232},
            'bar': {'timestamp': 231412323, 'DateModified': 324232},
        }
        set_index(client, 'testbucket', 'Music', sync_index)

        sync_client = S3SyncClient(client, 'testbucket', 'Music/')
        assert sync_client.sync_index == sync_index

    @moto.mock_s3
    def test_get_object_timestamp(self):
        client = boto3.client('s3')
        client.create_bucket(Bucket='testbucket')

        sync_index = {
            'foo': {'timestamp': 123213213, 'DateModified': 423232},
            'bar': {'timestamp': 231412323, 'DateModified': 324232},
        }
        set_index(client, 'testbucket', 'Music', sync_index)

        sync_client = S3SyncClient(client, 'testbucket', 'Music/')
        assert sync_client.get_object_timestamp('foo') == 123213213
        assert sync_client.get_object_timestamp('bar') == 231412323
        assert sync_client.get_object_timestamp('idontexist') is None

    @moto.mock_s3
    def test_set_object_timestamp(self):
        client = boto3.client('s3')
        client.create_bucket(Bucket='testbucket')

        sync_index = {
            'blargh': {'timestamp': 99999999, 'DateModified': 9999999},
        }
        set_index(client, 'testbucket', 'Music', sync_index)

        sync_client = S3SyncClient(client, 'testbucket', 'Music')
        sync_client.set_object_timestamp('blargh', 11111111)
        sync_client.set_object_timestamp('idontexist', 2323232)

        assert sync_client.get_object_timestamp('blargh') == 11111111
        assert sync_client.get_object_timestamp('idontexist') == 2323232
