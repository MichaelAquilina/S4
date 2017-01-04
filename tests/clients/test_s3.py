# -*- coding: utf-8 -*-
import datetime
import io

import boto3

from botocore.exceptions import ClientError

import freezegun

from moto import mock_s3

import pytest

from s3backup.clients import s3, SyncObject


def touch(client, bucket, key, timestamp=None):
    if timestamp is None:
        last_modified = datetime.datetime.utcnow()
    else:
        last_modified = datetime.datetime.utcfromtimestamp(timestamp)
    with freezegun.freeze_time(last_modified):
        client.put_object(Bucket=bucket, Key=key)


class TestS3SyncClient(object):
    @mock_s3
    def test_repr(self):
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')
        assert repr(client) == 'S3SyncClient<testbucket, foo/bar>'

    @mock_s3
    def test_put_get(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')

        input_object = SyncObject(io.BytesIO(b'woooooooooshhh'), 4000)
        frozen_time = datetime.datetime(2016, 10, 23, 10, 30, tzinfo=datetime.timezone.utc)

        # when
        with freezegun.freeze_time(frozen_time):
            client.put('something/woosh.gif', input_object)

        # then
        output_object = client.get('something/woosh.gif')
        assert output_object.fp.read() == b'woooooooooshhh'
        assert output_object.timestamp == s3.to_timestamp(frozen_time)

    @mock_s3
    def test_delete(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        s3_client.put_object(Bucket='testbucket', Key='foo/bar/war.png', Body='bang')

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')

        # when
        client.delete('war.png')

        # then
        with pytest.raises(ClientError) as exc:
            s3_client.head_object(Bucket='testbucket', Key='foo/bar/war.png')
        assert exc.value.response['Error']['Code'] == '404'

    @mock_s3
    def test_get_local_keys(self):
        # given
        s3_client = boto3.client(
            's3',
            aws_access_key_id='',
            aws_secret_access_key='',
            aws_session_token='',
        )
        s3_client.create_bucket(Bucket='testbucket')
        touch(s3_client, 'testbucket', 'foo/bar/war.png')
        touch(s3_client, 'testbucket', 'foo/bar/this/is/fine')
        touch(s3_client, 'testbucket', 'foo/bar/.index')
        touch(s3_client, 'testbucket', 'foo/ishouldnotshow.txt')

        client = s3.S3SyncClient(s3_client, 'testbucket', 'foo/bar')

        # when
        actual_output = client.get_local_keys()

        # then
        expected_output = ['war.png', 'this/is/fine']
        assert sorted(actual_output) == sorted(expected_output)
