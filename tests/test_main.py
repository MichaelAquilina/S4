# -*- coding: utf-8 -*-

import gzip
import json
import os
import shutil
import tempfile

import boto3

from faker import Faker

import moto

from s3backup import main
from s3backup.local_sync_client import LocalSyncClient, traverse
from s3backup.s3_sync_client import S3SyncClient


fake = Faker()


def setup_local_sync_client(target_folder, files):
    for key, timestamp in files.items():
        object_path = os.path.join(target_folder, key)
        with open(object_path, 'w') as fp:
            fp.write(fake.text())
            fp.flush()
            os.utime(object_path, (timestamp, timestamp))

    return LocalSyncClient(target_folder)


class TestPerformSync(object):

    def setup_method(self):
        self.target_folder = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.target_folder)

    @moto.mock_s3
    def test_perform_sync_empty_local(self):

        client = boto3.client('s3')
        client.create_bucket(Bucket='testbucket')

        index = {
            'foo/bar': {'timestamp': 2000},
            'skeleton/gloves.txt': {'timestamp': 2000},
            'hello.txt': {'timestamp': 2000},
        }
        for key in index.keys():
            client.put_object(Bucket='testbucket', Key='mybackup/{}'.format(key), Body=fake.text())
        client.put_object(
            Bucket='testbucket',
            Key='mybackup/.syncindex.json.gz',
            Body=gzip.compress(json.dumps(index).encode('utf-8')),
        )

        s3_client = S3SyncClient(client, 'testbucket', 'mybackup/')
        local_client = setup_local_sync_client(self.target_folder, files={})

        main.perform_sync(s3_client, local_client)

        object_list = client.list_objects(Bucket='testbucket', Prefix='mybackup/')
        actual_s3_keys = set(
            obj['Key'].replace('mybackup/', '', 1) for obj in object_list['Contents']
        )
        actual_s3_keys.remove('.syncindex.json.gz')

        actual_local_keys = set(traverse(self.target_folder))

        assert actual_local_keys == actual_s3_keys == {
            'foo/bar', 'skeleton/gloves.txt', 'hello.txt'
        }

        for key, metadata in index.items():
            local_timestamp = local_client.get_object_timestamp(key)
            s3_timestamp = s3_client.get_object_timestamp(key)
            target_timestamp = metadata['timestamp']
            assert local_timestamp == s3_timestamp == target_timestamp

    @moto.mock_s3
    def test_perform_sync_empty_s3(self):
        local_files = {'foo': 42323232, 'bar': 34243343}
        local_client = setup_local_sync_client(self.target_folder, files=local_files)

        client = boto3.client('s3')
        client.create_bucket(Bucket='testbucket')
        s3_client = S3SyncClient(client, 'testbucket', 'mybackup/')

        main.perform_sync(s3_client, local_client)

        object_list = client.list_objects(Bucket='testbucket', Prefix='mybackup/')
        actual_s3_keys = set(
            obj['Key'].replace('mybackup/', '', 1) for obj in object_list['Contents']
        )
        actual_s3_keys.remove('.syncindex.json.gz')

        actual_local_keys = set(traverse(self.target_folder))

        assert actual_local_keys == actual_s3_keys == {'foo', 'bar'}

        for key, target_timestamp in local_files.items():
            local_timestamp = local_client.get_object_timestamp(key)
            s3_timestamp = s3_client.get_object_timestamp(key)
            assert local_timestamp == s3_timestamp == target_timestamp
