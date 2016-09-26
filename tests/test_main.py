# -*- coding: utf-8 -*-

import os
import tempfile

import boto3

from faker import Faker

import moto

from s3backup import main
from s3backup.local_sync_client import LocalSyncClient
from s3backup.s3_sync_client import S3SyncClient


fake = Faker()


def setup_local_sync_client():
    target_folder = tempfile.mkdtemp()

    for i in range(20):
        object_path = os.path.join(target_folder, fake.file_name(category='text'))
        with open(object_path, 'w') as fp:
            fp.write(fake.text())

    return LocalSyncClient(target_folder)


def setup_s3_sync_client():
    bucket_name = fake.color_name()
    prefix = fake.color_name() + '/ '

    client = boto3.client('s3')
    client.create_bucket(Bucket=bucket_name)

    return S3SyncClient(client, bucket_name, prefix)


@moto.mock_s3
def test_perform_sync():
    local_client = setup_local_sync_client()
    s3_client = setup_s3_sync_client()

    main.perform_sync(s3_client, local_client)
