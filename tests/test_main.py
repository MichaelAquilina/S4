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


def setup_local_sync_client(target_folder, objects):
    for key, timestamp in objects.items():
        object_path = os.path.join(target_folder, key)
        with open(object_path, 'w') as fp:
            fp.write(fake.text())
            fp.flush()
            os.utime(object_path, (timestamp, timestamp))

    return LocalSyncClient(target_folder)


def setup_s3_sync_client(client, bucket, prefix, objects, create_index=True):
    client.create_bucket(Bucket='testbucket')
    for key in objects.keys():
        client.put_object(Bucket='testbucket', Key='mybackup/{}'.format(key), Body=fake.text())

    if create_index:
        index = {key: {'timestamp': timestamp} for key, timestamp in objects.items()}
        client.put_object(
            Bucket='testbucket',
            Key='mybackup/.syncindex.json.gz',
            Body=gzip.compress(json.dumps(index).encode('utf-8')),
        )
    return S3SyncClient(client, bucket, prefix)


class TestPerformSync(object):

    def setup_method(self):
        self.target_folder = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.target_folder)

    @moto.mock_s3
    def test_perform_sync_empty_local(self):
        client = boto3.client('s3')
        objects = {
            'foo/bar': 2020,
            'skeleton/gloves.txt': 5430,
            'hello.txt': 4000,
        }

        s3_client = setup_s3_sync_client(client, 'testbucket', 'mybackup/', objects)
        local_client = setup_local_sync_client(self.target_folder, objects={})

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

        for key, target_timestamp in objects.items():
            local_timestamp = local_client.get_object_timestamp(key)
            s3_timestamp = s3_client.get_object_timestamp(key)
            assert local_timestamp == s3_timestamp == target_timestamp

    @moto.mock_s3
    def test_perform_sync_empty_s3(self):
        client = boto3.client('s3')
        objects = {
            'foo': 42323232,
            'bar': 34243343,
        }

        local_client = setup_local_sync_client(self.target_folder, objects=objects)
        s3_client = setup_s3_sync_client(client, 'testbucket', 'mybackup/', {}, False)

        main.perform_sync(s3_client, local_client)

        object_list = client.list_objects(Bucket='testbucket', Prefix='mybackup/')
        actual_s3_keys = set(
            obj['Key'].replace('mybackup/', '', 1) for obj in object_list['Contents']
        )
        actual_s3_keys.remove('.syncindex.json.gz')

        actual_local_keys = set(traverse(self.target_folder))

        assert actual_local_keys == actual_s3_keys == {'foo', 'bar'}

        for key, target_timestamp in objects.items():
            local_timestamp = local_client.get_object_timestamp(key)
            s3_timestamp = s3_client.get_object_timestamp(key)
            assert local_timestamp == s3_timestamp == target_timestamp

    @moto.mock_s3
    def test_perform_sync_updates_local(self):
        client = boto3.client('s3')
        local_objects = {
            'foo': 40000000,
            'bar': 25000000,
        }
        s3_objects = {
            'foo': 40000000,
            'bar': 30000000,  # newer than local
            'baz': 90000000,  # does not exist on local
        }

        local_client = setup_local_sync_client(
            target_folder=self.target_folder,
            objects=local_objects,
        )
        s3_client = setup_s3_sync_client(
            client=client,
            bucket='testbucket',
            prefix='mybackup/',
            objects=s3_objects,
            create_index=True,
        )

        main.perform_sync(s3_client, local_client)

        object_list = client.list_objects(Bucket='testbucket', Prefix='mybackup/')
        actual_s3_keys = set(
            obj['Key'].replace('mybackup/', '', 1) for obj in object_list['Contents']
        )
        actual_s3_keys.remove('.syncindex.json.gz')

        actual_local_keys = set(traverse(self.target_folder))

        assert actual_local_keys == actual_s3_keys == {'foo', 'bar', 'baz'}
