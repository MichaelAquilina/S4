# -*- coding: utf-8 -*-

import shutil
import tempfile

import boto3

from faker import Faker

import moto

import pytest

from s4.clients.local import LocalSyncClient
from s4.clients.s3 import S3SyncClient

fake = Faker()


@pytest.yield_fixture
def local_client():
    folder = tempfile.mkdtemp()
    yield LocalSyncClient(folder)
    shutil.rmtree(folder)


@pytest.yield_fixture
def local_client_2():
    folder = tempfile.mkdtemp()
    yield LocalSyncClient(folder)
    shutil.rmtree(folder)


@pytest.yield_fixture
def s3_client():
    mock = moto.mock_s3()
    mock.start()
    boto_client = boto3.client(
        's3',
        aws_access_key_id='',
        aws_secret_access_key='',
        aws_session_token='',
    )
    bucket_name = fake.user_name()
    boto_client.create_bucket(Bucket=bucket_name)
    yield S3SyncClient(boto_client, bucket_name, fake.uri_path())
    mock.stop()
