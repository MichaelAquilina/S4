# -*- coding: utf-8 -*-

import logging
import os
import shutil
import tempfile
import uuid

import boto3

from faker import Faker

import mock

import moto

import pytest

from s4.clients.local import LocalSyncClient
from s4.clients.s3 import S3SyncClient

fake = Faker()


@pytest.fixture
def a_logger():
    result = logging.getLogger(str(uuid.uuid4()))
    result.setLevel(logging.INFO)
    result.handlers = []
    result.addHandler(logging.StreamHandler())
    return result


@pytest.fixture
def config_file():
    fd, temp_path = tempfile.mkstemp()
    mocker = mock.patch('s4.utils.CONFIG_FILE_PATH', temp_path)
    mocker.start()
    yield temp_path
    mocker.stop()
    os.close(fd)
    os.remove(temp_path)


@pytest.fixture
def local_client():
    folder = tempfile.mkdtemp()
    yield LocalSyncClient(folder)
    shutil.rmtree(folder)


@pytest.fixture
def local_client_2():
    folder = tempfile.mkdtemp()
    yield LocalSyncClient(folder)
    shutil.rmtree(folder)


@pytest.fixture
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
