# -*- coding: utf-8 -*-

import shutil
import tempfile

import boto3

import moto

import pytest

from s3backup.clients.local import LocalSyncClient
from s3backup.clients.s3 import S3SyncClient


@pytest.yield_fixture
def local_client():
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
    boto_client.create_bucket(Bucket='testbucket')
    yield S3SyncClient(boto_client, 'testbucket', 'foo')
    mock.stop()
