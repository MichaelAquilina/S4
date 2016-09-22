# -*- coding: utf-8 -*-

import gzip
import json

import boto3
import moto

from s3backup.s3_sync_client import S3SyncClient


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

        client.put_object(
            Bucket='testbucket',
            Key='Music/.syncindex.json.gz',
            Body=gzip.compress(json.dumps(sync_index).encode('utf-8'))
        )

        sync_client = S3SyncClient(client, 'testbucket', 'Music/')
        assert sync_client.sync_index == sync_index
