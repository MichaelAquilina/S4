# -*- coding: utf-8 -*-

import os
import shutil
import tempfile

import boto3

from faker import Faker

import moto

from s3backup import main
from s3backup.local_sync_client import LocalSyncClient
from s3backup.s3_sync_client import S3SyncClient


fake = Faker()


def setup_local_sync_client(target_folder, file_names=None):
    if file_names is None:
        file_names = [fake.file_name(category='text') for _ in range(20)]

    for key in file_names:
        object_path = os.path.join(target_folder, key)
        with open(object_path, 'w') as fp:
            fp.write(fake.text())

    return LocalSyncClient(target_folder)


class TestPerformSync(object):

    def setup_method(self):
        self.target_folder = tempfile.mkdtemp()

    def teardown_method(self):
        shutil.rmtree(self.target_folder)

    @moto.mock_s3
    def test_perform_sync(self):
        local_client = setup_local_sync_client(self.target_folder, file_names=['foo', 'bar'])

        client = boto3.client('s3')
        client.create_bucket(Bucket='testbucket')
        s3_client = S3SyncClient(client, 'testbucket', 'mybackup/')

        main.perform_sync(s3_client, local_client)

        object_list = client.list_objects(Bucket='testbucket', Prefix='mybackup/')

        actual_keys = set(obj['Key'] for obj in object_list['Contents'])
        expected_keys = {'mybackup/foo', 'mybackup/bar', 'mybackup/.syncindex.json.gz'}
        assert actual_keys == expected_keys
